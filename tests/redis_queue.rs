use async_trait::async_trait;
use queue_workers::{
    error::QueueWorkerError,
    job::Job,
    queue::{Queue, QueueType},
    redis_queue::RedisQueue,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

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
        Ok(format!(
            "Processed job {} with payload {}",
            self.id, self.payload
        ))
    }
}

#[tokio::test]
async fn test_redis_queue_push_pop() {
    // Make sure Redis is running on this URL for tests
    let redis_url = "redis://127.0.0.1:6379";
    let queue =
        RedisQueue::<TestJob>::new(redis_url, "test_queue").expect("Failed to create Redis queue");

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
    assert!(matches!(
        empty_result,
        Err(QueueWorkerError::JobNotFound(_))
    ));
}

#[tokio::test]
async fn test_queue_types() {
    let redis_url = "redis://127.0.0.1:6379";

    // Test FIFO queue
    let fifo_queue = RedisQueue::<TestJob>::new(redis_url, "test_fifo_queue")
        .expect("Failed to create FIFO queue");

    // Test LIFO queue
    let lifo_queue =
        RedisQueue::<TestJob>::with_type(redis_url, "test_lifo_queue", QueueType::LIFO)
            .expect("Failed to create LIFO queue");

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
    let result = RedisQueue::<TestJob>::new("invalid-url", "test_queue");
    assert!(matches!(result, Err(QueueWorkerError::ConnectionError(_))));
}

#[tokio::test]
async fn test_empty_queue_name() {
    let result = RedisQueue::<TestJob>::new("redis://127.0.0.1:6379", "");
    assert!(matches!(result, Err(QueueWorkerError::InvalidJobData(_))));
}

#[tokio::test]
async fn test_concurrent_queue_access() {
    let redis_url = "redis://127.0.0.1:6379";
    let queue = RedisQueue::<TestJob>::new(redis_url, "test_concurrent")
        .expect("Failed to create Redis queue");

    let mut handles = vec![];
    let job_count = 100;

    // Spawn multiple tasks pushing jobs
    for i in 0..job_count {
        let queue_clone = queue.clone();
        let handle = tokio::spawn(async move {
            let job = TestJob {
                id: format!("concurrent-{}", i),
                payload: "test".to_string(),
            };
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
    let redis_url = "redis://127.0.0.1:6379";
    let queue_name = "test_persistence";

    // Create first queue instance
    let queue1 =
        RedisQueue::<TestJob>::new(redis_url, queue_name).expect("Failed to create first queue");

    // Push a job
    let job = TestJob {
        id: "persist-1".to_string(),
        payload: "test payload".to_string(),
    };
    queue1.push(job.clone()).await.expect("Failed to push job");

    // Create second queue instance
    let queue2 =
        RedisQueue::<TestJob>::new(redis_url, queue_name).expect("Failed to create second queue");

    // Pop job from second instance
    let received_job = queue2.pop().await.expect("Failed to pop job");
    assert_eq!(received_job.id, job.id);
    assert_eq!(received_job.payload, job.payload);
}

#[tokio::test]
async fn test_queue_behavior_under_load() {
    let redis_url = "redis://127.0.0.1:6379";
    let queue =
        RedisQueue::<TestJob>::new(redis_url, "test_load").expect("Failed to create Redis queue");

    // Push many jobs rapidly
    for i in 0..1000 {
        let job = TestJob {
            id: format!("load-{}", i),
            payload: "test".to_string(),
        };
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
    let redis_url = "redis://127.0.0.1:6379";
    let queue =
        RedisQueue::<TestJob>::new(redis_url, "test_empty").expect("Failed to create Redis queue");

    // Test pop on empty queue
    let result = queue.pop().await;
    assert!(matches!(result, Err(QueueWorkerError::JobNotFound(_))));
}

#[tokio::test]
async fn test_queue_type_switching() {
    let redis_url = "redis://127.0.0.1:6379";
    let queue_name = "test_switching";

    // Create FIFO queue
    let fifo_queue = RedisQueue::<TestJob>::with_type(redis_url, queue_name, QueueType::FIFO)
        .expect("Failed to create FIFO queue");

    // Push some jobs
    for i in 0..3 {
        let job = TestJob {
            id: format!("switch-{}", i),
            payload: "test".to_string(),
        };
        fifo_queue.push(job).await.expect("Failed to push job");
    }

    // Create LIFO queue with same name
    let lifo_queue = RedisQueue::<TestJob>::with_type(redis_url, queue_name, QueueType::LIFO)
        .expect("Failed to create LIFO queue");

    // Verify order is now LIFO
    let job1 = lifo_queue.pop().await.expect("Failed to pop job 1");
    assert_eq!(job1.id, "switch-2");
}
