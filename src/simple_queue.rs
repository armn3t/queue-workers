use async_trait::async_trait;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use std::time::Duration;

use crate::{
    error::QueueWorkerError,
    job::{Job, JobWrapper},
    metrics::Metrics,
    queue::{Queue, QueueType},
};

/// A simple queue implementation using Redis.
/// This queue serializes jobs to JSON and stores them in Redis.
pub struct SimpleQueue<J> {
    client: Client,
    queue_name: String,
    queue_type: QueueType,
    metrics: Option<std::sync::Arc<dyn Metrics>>,
    _phantom: std::marker::PhantomData<J>,
}

impl<J> SimpleQueue<J> {
    /// Set metrics collector for this queue
    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get the current queue depth (number of jobs waiting)
    pub async fn get_depth(&self) -> Result<u64, QueueWorkerError> {
        let mut conn = self.get_connection().await?;
        let len: u64 = conn
            .llen(&self.queue_name)
            .await
            .map_err(QueueWorkerError::RedisError)?;

        // Record the queue depth metric if metrics are configured
        if let Some(metrics) = &self.metrics {
            metrics.record_queue_depth(&self.queue_name, len, &[]);
        }

        Ok(len)
    }
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
            metrics: None,
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
            metrics: self.metrics.clone(),
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

    fn queue_name(&self) -> &str {
        &self.queue_name
    }

    async fn push(&self, job: Self::JobType) -> Result<(), QueueWorkerError> {
        let mut conn = self.get_connection().await?;

        // Get the job type for metrics
        let job_type = job.job_type();

        // Create a wrapper that includes the job type
        let wrapper = JobWrapper::new(job);

        // Serialize the wrapper
        let job_data =
            serde_json::to_string(&wrapper).map_err(QueueWorkerError::SerializationError)?;

        conn.lpush::<&String, String, ()>(&self.queue_name, job_data)
            .await
            .map_err(QueueWorkerError::RedisError)?;

        // Record metrics if configured
        if let Some(metrics) = &self.metrics {
            metrics.record_job_enqueued(job_type, &self.queue_name);

            // Update queue depth after push
            if let Ok(depth) = conn.llen::<&String, u64>(&self.queue_name).await {
                metrics.record_queue_depth(&self.queue_name, depth, &[]);
            }
        }

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

        // Get the job for return
        let job = wrapper.into_job_data();

        // Record metrics if configured
        if let Some(metrics) = &self.metrics {
            metrics.record_job_dequeued(job.job_type(), &self.queue_name);

            // Update queue depth after pop
            if let Ok(depth) = conn.llen::<&String, u64>(&self.queue_name).await {
                metrics.record_queue_depth(&self.queue_name, depth, &[]);
            }
        }

        // Return the job data
        Ok(job)
    }
}
