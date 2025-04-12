pub mod concurrent_worker;
pub mod error;
pub mod job;
pub mod queue;
pub mod redis_queue;
pub mod worker;

#[cfg(test)]
mod tests {
    use super::job::Job;
    use super::queue::Queue;
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Debug)]
    struct TestJob {
        id: String,
        delay: Duration,
        should_fail: bool,
    }

    #[async_trait::async_trait]
    impl Job for TestJob {
        type Output = String;
        type Error = String;

        async fn execute(&self) -> Result<Self::Output, Self::Error> {
            sleep(self.delay).await;

            if self.should_fail {
                Err(format!("Job {} failed", self.id))
            } else {
                Ok(format!("Job {} completed", self.id))
            }
        }
    }

    // Mock queue implementation for testing
    struct MockQueue {
        jobs: std::sync::Mutex<Vec<TestJob>>,
    }

    #[async_trait::async_trait]
    impl queue::Queue for MockQueue {
        type JobType = TestJob;

        async fn push(&self, job: Self::JobType) -> Result<(), error::QueueWorkerError> {
            let mut jobs = self.jobs.lock().unwrap();
            jobs.push(job);
            Ok(())
        }

        async fn pop(&self) -> Result<Self::JobType, error::QueueWorkerError> {
            let mut jobs = self.jobs.lock().unwrap();
            jobs.pop()
                .ok_or_else(|| error::QueueWorkerError::JobNotFound("Queue empty".to_string()))
        }
    }

    #[tokio::test]
    async fn test_job_execution_success() {
        let job = TestJob {
            id: "test-1".to_string(),
            delay: Duration::from_millis(100),
            should_fail: false,
        };

        let result = job.execute().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Job test-1 completed");
    }

    #[tokio::test]
    async fn test_job_execution_failure() {
        let job = TestJob {
            id: "test-2".to_string(),
            delay: Duration::from_millis(100),
            should_fail: true,
        };

        let result = job.execute().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Job test-2 failed");
    }

    #[tokio::test]
    async fn test_queue_push_pop() {
        let queue = MockQueue {
            jobs: std::sync::Mutex::new(Vec::new()),
        };

        let job = TestJob {
            id: "test-3".to_string(),
            delay: Duration::from_millis(100),
            should_fail: false,
        };

        // Test push
        let push_result = queue.push(job).await;
        assert!(push_result.is_ok());

        // Test pop
        let pop_result = queue.pop().await;
        assert!(pop_result.is_ok());

        // Queue should be empty now
        let empty_pop = queue.pop().await;
        assert!(empty_pop.is_err());
        assert!(matches!(
            empty_pop.unwrap_err(),
            error::QueueWorkerError::JobNotFound(_)
        ));
    }
}
