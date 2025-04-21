use async_trait::async_trait;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use std::time::Duration;

use crate::{
    error::QueueWorkerError,
    job::{Job, JobWrapper},
    queue::{Queue, QueueType},
};

/// A simple queue implementation using Redis.
/// This queue serializes jobs to JSON and stores them in Redis.
pub struct SimpleQueue<J> {
    client: Client,
    queue_name: String,
    queue_type: QueueType,
    _phantom: std::marker::PhantomData<J>,
}

impl<J> SimpleQueue<J> {
    /// Create a new SimpleQueue with the default queue type (FIFO).
    pub fn new(redis_url: &str, queue_name: &str) -> Result<Self, QueueWorkerError> {
        Self::with_type(redis_url, queue_name, QueueType::default())
    }

    /// Create a new SimpleQueue with a specific queue type.
    pub fn with_type(
        redis_url: &str,
        queue_name: &str,
        queue_type: QueueType,
    ) -> Result<Self, QueueWorkerError> {
        if redis_url.is_empty() {
            return Err(QueueWorkerError::ConnectionError(
                "Redis URL cannot be empty".to_string(),
            ));
        }
        if queue_name.is_empty() {
            return Err(QueueWorkerError::InvalidJobData(
                "Queue name cannot be empty".to_string(),
            ));
        }

        let client = Client::open(redis_url)
            .map_err(|e| QueueWorkerError::ConnectionError(e.to_string()))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            queue_type,
            _phantom: std::marker::PhantomData,
        })
    }

    async fn get_connection(&self) -> Result<MultiplexedConnection, QueueWorkerError> {
        match tokio::time::timeout(
            Duration::from_secs(5),
            self.client.get_multiplexed_async_connection(),
        )
        .await
        {
            Ok(conn_result) => {
                conn_result.map_err(|e| QueueWorkerError::ConnectionError(e.to_string()))
            }
            Err(_) => Err(QueueWorkerError::TimeoutError),
        }
    }
}

impl<J> Clone for SimpleQueue<J> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            queue_name: self.queue_name.clone(),
            queue_type: self.queue_type,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<J> Queue for SimpleQueue<J>
where
    J: Job + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync,
{
    type JobType = J;

    async fn push(&self, job: Self::JobType) -> Result<(), QueueWorkerError> {
        let mut conn = self.get_connection().await?;

        // Create a wrapper that includes the job type
        let wrapper = JobWrapper::new(job);

        // Serialize the wrapper
        let job_data =
            serde_json::to_string(&wrapper).map_err(QueueWorkerError::SerializationError)?;

        conn.lpush::<&String, String, ()>(&self.queue_name, job_data)
            .await
            .map_err(QueueWorkerError::RedisError)?;

        Ok(())
    }

    async fn pop(&self) -> Result<Self::JobType, QueueWorkerError> {
        let mut conn = self.get_connection().await?;

        let job_data: Option<String> = match self.queue_type {
            QueueType::FIFO => conn
                .rpop::<&String, Option<String>>(&self.queue_name, None)
                .await
                .map_err(QueueWorkerError::RedisError)?,
            QueueType::LIFO => conn
                .lpop::<&String, Option<String>>(&self.queue_name, None)
                .await
                .map_err(QueueWorkerError::RedisError)?,
        };

        let job_data = job_data.ok_or_else(|| {
            QueueWorkerError::JobNotFound(format!("Queue '{}' is empty", self.queue_name))
        })?;

        // Deserialize the wrapper first
        let wrapper: JobWrapper<J> =
            serde_json::from_str(&job_data).map_err(QueueWorkerError::SerializationError)?;

        // Return the job data
        Ok(wrapper.into_job_data())
    }
}
