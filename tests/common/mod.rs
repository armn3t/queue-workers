use async_trait::async_trait;
use queue_workers::{error::QueueWorkerError, job::Job, queue::Queue};
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::Level;
use tracing_subscriber::{EnvFilter, fmt, util::SubscriberInitExt};

static INIT: Once = Once::new();

/// Initializes logging for integration tests with a consistent configuration.
/// This function is safe to call multiple times as it will only initialize logging once.
///
/// If the environment variable DISABLE_TEST_LOGGING=1 is set, logging will be disabled.
pub fn init_test_logging() {
    INIT.call_once(|| {
        // Check if logging should be disabled
        if let Ok(disable_logging) = std::env::var("DISABLE_TEST_LOGGING") {
            if disable_logging == "1" {
                // Initialize with a filter that discards all logs
                let filter = EnvFilter::new("off");
                let fmt_layer = fmt::layer().without_time();

                use tracing_subscriber::layer::SubscriberExt;
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt_layer)
                    .init();
                return;
            }
        }

        // Create an environment filter that can be controlled via RUST_LOG
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("queue_workers=debug,redis=debug,tower=warn,test=debug")
        });

        // Create a JSON formatting layer for test output
        let json_layer = fmt::layer()
            .json()
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_target(true)
            .with_current_span(true)
            .with_span_list(true);

        // Combine layers and initialize the subscriber
        use tracing_subscriber::layer::SubscriberExt;
        tracing_subscriber::registry()
            .with(filter)
            .with(json_layer)
            .init();
    });
}

#[derive(Clone)]
pub struct TestJob {
    pub attempts: Arc<Mutex<u32>>,
    pub should_fail: bool,
    pub retry_conditions: RetryCondition,
    pub job_duration: Duration,
    pub completed: Arc<AtomicBool>,
    pub execution_tracker:
        Option<Arc<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>>,
    pub started_execution: Arc<AtomicBool>,
    pub before_retry_notifier: Option<Arc<tokio::sync::Notify>>,
    pub execution_complete_notifier: Option<Arc<tokio::sync::Notify>>,
    pub retry_count: Arc<AtomicUsize>,
}

impl std::fmt::Debug for TestJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestJob")
            .field("attempts", &self.attempts)
            .field("should_fail", &self.should_fail)
            .field("retry_conditions", &self.retry_conditions)
            .field("job_duration", &self.job_duration)
            .field("completed", &self.completed)
            .field("started_execution", &self.started_execution)
            .field("retry_count", &self.retry_count)
            .field("execution_tracker", &format!("<function>"))
            .finish()
    }
}

impl TestJob {
    pub fn new() -> Self {
        Self {
            attempts: Arc::new(Mutex::new(0)),
            should_fail: false,
            retry_conditions: RetryCondition::Never,
            job_duration: Duration::from_millis(0),
            completed: Arc::new(AtomicBool::new(false)),
            execution_tracker: None,
            started_execution: Arc::new(AtomicBool::new(false)),
            before_retry_notifier: None,
            execution_complete_notifier: None,
            retry_count: Arc::new(AtomicUsize::new(0)),
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

    pub fn with_execution_tracker<F, Fut>(mut self, tracker: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: futures::Future<Output = ()> + Send + 'static,
    {
        self.execution_tracker = Some(Arc::new(move || Box::pin(tracker())));
        self
    }

    pub fn with_concurrent_execution_tracker<F, Fut>(mut self, tracker: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: futures::Future<Output = ()> + Send + 'static,
    {
        self.execution_tracker = Some(Arc::new(move || Box::pin(tracker())));
        self
    }

    pub fn with_before_retry_notifier(mut self, notifier: Arc<tokio::sync::Notify>) -> Self {
        self.before_retry_notifier = Some(notifier);
        self
    }

    pub fn with_execution_complete_notifier(mut self, notifier: Arc<tokio::sync::Notify>) -> Self {
        self.execution_complete_notifier = Some(notifier);
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
            execution_tracker: None,
            started_execution: Arc::new(AtomicBool::new(false)),
            before_retry_notifier: None,
            execution_complete_notifier: None,
            retry_count: Arc::new(AtomicUsize::new(0)),
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
        self.started_execution.store(true, Ordering::SeqCst);

        let mut attempts = self.attempts.lock().await;
        *attempts += 1;
        println!("Job attempt: {}", *attempts);

        // Execute any tracker if present
        if let Some(tracker) = &self.execution_tracker {
            (tracker)().await;
        } else {
            tokio::time::sleep(self.job_duration).await;
        }

        println!(
            "Job duration complete. Job should fail: {}",
            self.should_fail
        );

        let result = if self.should_fail {
            Err("Job failed".to_string())
        } else {
            self.completed.store(true, Ordering::SeqCst);
            Ok(())
        };

        // Notify that execution is complete
        if let Some(notifier) = &self.execution_complete_notifier {
            notifier.notify_one();
        }

        result
    }

    fn should_retry(&self, _error: &Self::Error, attempt: u32) -> bool {
        // Increment retry count
        self.retry_count.fetch_add(1, Ordering::SeqCst);

        // Notify before retry if notifier is set
        if let Some(notifier) = &self.before_retry_notifier {
            notifier.notify_one();
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::{debug, error, info, warn};

    #[test]
    fn test_logging_levels() {
        init_test_logging();

        error!("This is an error message");
        warn!("This is a warning message");
        info!("This is an info message");
        debug!("This is a debug message");

        // Test structured logging
        let test_value = 42;
        info!(
            test_value,
            message = "This is a structured log message",
            additional_field = "extra info"
        );
    }

    #[test]
    fn test_span_logging() {
        init_test_logging();

        let span = tracing::span!(Level::INFO, "test_span", test_field = "test_value");
        let _guard = span.enter();

        info!("This message is within the span");
        debug!("This debug message should include span context");
    }
}
