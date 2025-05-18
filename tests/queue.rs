mod common;
use common::{TestJob, TestQueue};
use queue_workers::error;
use queue_workers::jobs::job::Job;
use queue_workers::queues::queue::Queue;

#[tokio::test]
async fn test_job_execution_success() {
    let job = TestJob::new();

    let result = job.execute().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ());
}

#[tokio::test]
async fn test_job_execution_failure() {
    let job = TestJob::new().with_should_fail(true);

    let result = job.execute().await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "Job failed");
}

#[tokio::test]
async fn test_queue_push_pop() {
    let queue = TestQueue {
        jobs: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
    };

    let job = TestJob::new();

    let push_result = queue.push(job).await;
    assert!(push_result.is_ok());

    let pop_result = queue.pop().await;
    assert!(pop_result.is_ok());

    let empty_pop = queue.pop().await;
    assert!(empty_pop.is_err());
    assert!(matches!(
        empty_pop.unwrap_err(),
        error::QueueWorkerError::JobNotFound(_)
    ));
}
