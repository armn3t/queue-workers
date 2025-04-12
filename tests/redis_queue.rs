use queue_workers::{
    error::QueueWorkerError, job::Job, queue::{Queue, QueueType}, redis_queue::RedisQueue
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[tokio::test]
async fn test_queue_types() {
    let redis_url = "redis://127.0.0.1:6379";
    
    // Test FIFO queue
    let fifo_queue = RedisQueue::<TestJob>::new(
        redis_url,
        "test_fifo_queue",
    ).expect("Failed to create FIFO queue");

    // Test LIFO queue
    let lifo_queue = RedisQueue::<TestJob>::with_type(
        redis_url,
        "test_lifo_queue",
        QueueType::LIFO,
    ).expect("Failed to create LIFO queue");

    // Create test jobs
    let jobs = vec![
        TestJob {
            id: "1".to_string(),
            payload: "first".to_string(),
        },
        TestJob {
            id: "2".to_string(),
            payload: "second".to_string(),
        },
        TestJob {
            id: "3".to_string(),
            payload: "third".to_string(),
        },
    ];

    // Push jobs to both queues
    for job in jobs {
        fifo_queue.push(job.clone()).await.expect("Failed to push to FIFO queue");
        lifo_queue.push(job).await.expect("Failed to push to LIFO queue");
    }

    // Test FIFO order (should be 1, 2, 3)
    let first = fifo_queue.pop().await.expect("Failed to pop from FIFO queue");
    assert_eq!(first.id, "1");
    let second = fifo_queue.pop().await.expect("Failed to pop from FIFO queue");
    assert_eq!(second.id, "2");
    let third = fifo_queue.pop().await.expect("Failed to pop from FIFO queue");
    assert_eq!(third.id, "3");

    // Test LIFO order (should be 3, 2, 1)
    let first = lifo_queue.pop().await.expect("Failed to pop from LIFO queue");
    assert_eq!(first.id, "3");
    let second = lifo_queue.pop().await.expect("Failed to pop from LIFO queue");
    assert_eq!(second.id, "2");
    let third = lifo_queue.pop().await.expect("Failed to pop from LIFO queue");
    assert_eq!(third.id, "1");
}
