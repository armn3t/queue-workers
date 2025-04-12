use async_trait::async_trait;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::QueueWorkerError,
    job::Job,
    queue::Queue,
};

pub struct RedisQueue<J> {
    client: Client,
    queue_name: String,
    _phantom: std::marker::PhantomData<J>,
}

impl<J> RedisQueue<J> {
    pub fn new(redis_url: &str, queue_name: &str) -> Result<Self, QueueWorkerError> {
        let client = Client::open(redis_url)
            .map_err(|e| QueueWorkerError::ConnectionError(e.to_string()))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            _phantom: std::marker::PhantomData,
        })
    }

    async fn get_connection(&self) -> Result<MultiplexedConnection, QueueWorkerError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| QueueWorkerError::ConnectionError(e.to_string()))
    }
}

#[async_trait]
impl<J> Queue for RedisQueue<J>
where
    J: Job + Serialize + DeserializeOwned + Send + Sync,
{
    type JobType = J;

    async fn push(&self, job: Self::JobType) -> Result<(), QueueWorkerError> {
        let mut conn = self.get_connection().await?;
        
        // Serialize the job
        let job_data = serde_json::to_string(&job)
            .map_err(QueueWorkerError::SerializationError)?;

        // Push to Redis list
        conn.rpush::<&String, String, ()>(&self.queue_name, job_data)
            .await
            .map_err(QueueWorkerError::RedisError)?;

        Ok(())
    }

    async fn pop(&self) -> Result<Self::JobType, QueueWorkerError> {
        let mut conn = self.get_connection().await?;

        // Pop from Redis list (left side)
        let job_data: String = conn
            .lpop::<&String, Option<String>>(&self.queue_name, None)
            .await
            .map_err(QueueWorkerError::RedisError)?
            .ok_or_else(|| QueueWorkerError::JobNotFound("Queue is empty".to_string()))?;

        // Deserialize the job
        serde_json::from_str(&job_data)
            .map_err(QueueWorkerError::SerializationError)
    }
}