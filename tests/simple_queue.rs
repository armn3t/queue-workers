use async_trait::async_trait;
use queue_workers::{
    error::QueueWorkerError,
    job::Job,
    queue::{Queue, QueueType},
    simple_queue::SimpleQueue,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

fn get_redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestJob {
    id: String,
    payload: String,
    job_type: String,
}

impl TestJob {
    fn new(id: &str, payload: &str) -> Self {
        Self {
            id: id.to_string(),
            payload: payload.to_string(),
            job_type: "TestJob".to_string(),
        }
    }
}

#[async_trait]
impl Job for TestJob {
    type Output = String;
    type Error = String;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        Ok(format!(
            "Processed job {} with payload {}",
            self.id, self.payload
        ))
    }

    // Override the default job_type implementation to use the job_type field
    fn job_type(&self) -> &'static str {
        // This is a bit of a hack, but it works for testing
        // In a real implementation, you would use a static string
        Box::leak(self.job_type.clone().into_boxed_str())
    }
}

#[tokio::test]
async fn test_simple_queue_push_pop() {
    let redis_url = get_redis_url();
    let queue =
        SimpleQueue::<TestJob>::new(&redis_url, &format!("test_queue-{}", uuid::Uuid::new_v4()))
            .expect("Failed to create Simple queue");

    // Create a test job
    let job = TestJob::new("test-1", "test payload");

    // Push job to queue
    queue.push(job).await.expect("Failed to push job");

    // Pop job from queue
    let retrieved_job = queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.id, "test-1");
    assert_eq!(retrieved_job.payload, "test payload");
    assert_eq!(retrieved_job.job_type, "TestJob");

    // Queue should be empty now
    let empty_result = queue.pop().await;
    assert!(matches!(
        empty_result,
        Err(QueueWorkerError::JobNotFound(_))
    ));
}

#[tokio::test]
async fn test_queue_types() {
    let redis_url = get_redis_url();

    // Test FIFO queue
    let fifo_queue = SimpleQueue::<TestJob>::new(&redis_url, "test_fifo_queue")
        .expect("Failed to create FIFO queue");

    // Test LIFO queue
    let lifo_queue =
        SimpleQueue::<TestJob>::with_type(&redis_url, "test_lifo_queue", QueueType::LIFO)
            .expect("Failed to create LIFO queue");

    // Create test jobs
    let jobs = vec![
        TestJob::new("1", "first"),
        TestJob::new("2", "second"),
        TestJob::new("3", "third"),
    ];

    // Push jobs to both queues
    for job in jobs {
        fifo_queue
            .push(job.clone())
            .await
            .expect("Failed to push to FIFO queue");
        lifo_queue
            .push(job)
            .await
            .expect("Failed to push to LIFO queue");
    }

    // Test FIFO order (should be 1, 2, 3)
    let first = fifo_queue
        .pop()
        .await
        .expect("Failed to pop from FIFO queue");
    assert_eq!(first.id, "1");
    let second = fifo_queue
        .pop()
        .await
        .expect("Failed to pop from FIFO queue");
    assert_eq!(second.id, "2");
    let third = fifo_queue
        .pop()
        .await
        .expect("Failed to pop from FIFO queue");
    assert_eq!(third.id, "3");

    // Test LIFO order (should be 3, 2, 1)
    let first = lifo_queue
        .pop()
        .await
        .expect("Failed to pop from LIFO queue");
    assert_eq!(first.id, "3");
    let second = lifo_queue
        .pop()
        .await
        .expect("Failed to pop from LIFO queue");
    assert_eq!(second.id, "2");
    let third = lifo_queue
        .pop()
        .await
        .expect("Failed to pop from LIFO queue");
    assert_eq!(third.id, "1");
}

#[tokio::test]
async fn test_invalid_redis_url() {
    let result = SimpleQueue::<TestJob>::new("invalid-url", "test_queue");
    assert!(matches!(result, Err(QueueWorkerError::ConnectionError(_))));
}

#[tokio::test]
async fn test_empty_queue_name() {
    let redis_url = get_redis_url();
    let result = SimpleQueue::<TestJob>::new(&redis_url, "");
    assert!(matches!(result, Err(QueueWorkerError::InvalidJobData(_))));
}

#[tokio::test]
async fn test_concurrent_queue_access() {
    let redis_url = get_redis_url();
    let queue = SimpleQueue::<TestJob>::new(&redis_url, "test_concurrent")
        .expect("Failed to create Simple queue");

    let mut handles = vec![];
    let job_count = 100;

    // Spawn multiple tasks pushing jobs
    for i in 0..job_count {
        let queue_clone = queue.clone();
        let handle = tokio::spawn(async move {
            let job = TestJob::new(&format!("concurrent-{}", i), "test");
            queue_clone.push(job).await
        });
        handles.push(handle);
    }

    // Wait for all pushes to complete
    for handle in handles {
        handle.await.expect("Task failed").expect("Push failed");
    }

    // Verify all jobs were pushed
    let mut received_count = 0;
    while let Ok(_) = queue.pop().await {
        received_count += 1;
    }
    assert_eq!(received_count, job_count);
}

#[tokio::test]
async fn test_queue_persistence() {
    let redis_url = get_redis_url();
    let queue_name = "test_persistence";

    // Create first queue instance
    let queue1 =
        SimpleQueue::<TestJob>::new(&redis_url, queue_name).expect("Failed to create first queue");

    // Push a job
    let job = TestJob::new("persist-1", "test payload");
    queue1.push(job.clone()).await.expect("Failed to push job");

    // Create second queue instance
    let queue2 =
        SimpleQueue::<TestJob>::new(&redis_url, queue_name).expect("Failed to create second queue");

    // Pop job from second instance
    let received_job = queue2.pop().await.expect("Failed to pop job");
    assert_eq!(received_job.id, job.id);
    assert_eq!(received_job.payload, job.payload);
}

#[tokio::test]
async fn test_queue_behavior_under_load() {
    let redis_url = get_redis_url();
    let queue = SimpleQueue::<TestJob>::new(&redis_url, "test_load")
        .expect("Failed to create Simple queue");

    // Push many jobs rapidly
    for i in 0..1000 {
        let job = TestJob::new(&format!("load-{}", i), "test");
        queue.push(job).await.expect("Failed to push job");
    }

    // Pop jobs with minimal delay
    let mut count = 0;
    while let Ok(_) = queue.pop().await {
        count += 1;
        sleep(Duration::from_micros(10)).await;
    }
    assert_eq!(count, 1000);
}

#[tokio::test]
async fn test_queue_empty_behavior() {
    let redis_url = get_redis_url();
    let queue = SimpleQueue::<TestJob>::new(&redis_url, "test_empty")
        .expect("Failed to create Simple queue");

    // Test pop on empty queue
    let result = queue.pop().await;
    assert!(matches!(result, Err(QueueWorkerError::JobNotFound(_))));
}

#[tokio::test]
async fn test_queue_type_switching() {
    let redis_url = get_redis_url();
    let queue_name = "test_switching";

    // Create FIFO queue
    let fifo_queue = SimpleQueue::<TestJob>::with_type(&redis_url, queue_name, QueueType::FIFO)
        .expect("Failed to create FIFO queue");

    // Push some jobs
    for i in 0..3 {
        let job = TestJob::new(&format!("switch-{}", i), "test");
        fifo_queue.push(job).await.expect("Failed to push job");
    }

    // Create LIFO queue with same name
    let lifo_queue = SimpleQueue::<TestJob>::with_type(&redis_url, queue_name, QueueType::LIFO)
        .expect("Failed to create LIFO queue");

    // Verify order is now LIFO
    let job1 = lifo_queue.pop().await.expect("Failed to pop job 1");
    assert_eq!(job1.id, "switch-2");
}

#[tokio::test]
async fn test_custom_job_type() {
    let redis_url = get_redis_url();
    let queue = SimpleQueue::<TestJob>::new(&redis_url, "test_job_type")
        .expect("Failed to create Simple queue");

    // Create a job with a custom type
    let mut job = TestJob::new("custom-type", "test payload");
    job.job_type = "CustomJobType".to_string();

    // Push job to queue
    queue.push(job).await.expect("Failed to push job");

    // Pop job from queue
    let retrieved_job = queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.job_type, "CustomJobType");
}
