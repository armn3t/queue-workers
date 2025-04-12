use queue_workers::{
    error::QueueWorkerError, job::Job, queue::Queue, redis_queue::RedisQueue
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct TestJob {
    id: String,
    payload: String,
}

#[async_trait]
impl Job for TestJob {
    type Output = String;
    type Error = String;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        Ok(format!("Processed job {} with payload {}", self.id, self.payload))
    }
}

#[tokio::test]
async fn test_redis_queue_push_pop() {
    // Make sure Redis is running on this URL for tests
    let redis_url = "redis://127.0.0.1:6379";
    let queue = RedisQueue::<TestJob>::new(redis_url, "test_queue")
        .expect("Failed to create Redis queue");

    // Create a test job
    let job = TestJob {
        id: "test-1".to_string(),
        payload: "test payload".to_string(),
    };

    // Push job to queue
    queue.push(job).await.expect("Failed to push job");

    // Pop job from queue
    let retrieved_job = queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.id, "test-1");
    assert_eq!(retrieved_job.payload, "test payload");

    // Queue should be empty now
    let empty_result = queue.pop().await;
    assert!(matches!(empty_result, Err(QueueWorkerError::JobNotFound(_))));
}