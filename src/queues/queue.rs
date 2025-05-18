use async_trait::async_trait;

use crate::{error, jobs::job::Job};

#[derive(Clone, Copy, Debug)]
pub enum QueueType {
    FIFO, // First In, First Out (using LPUSH/RPOP)
    LIFO, // Last In, First Out (using LPUSH/LPOP)
}

impl Default for QueueType {
    fn default() -> Self {
        Self::FIFO
    }
}

#[async_trait]
pub trait Queue: Send + Sync {
    type JobType: Job;

    async fn push(&self, job: Self::JobType) -> Result<(), error::QueueWorkerError>;

    async fn pop(&self) -> Result<Self::JobType, error::QueueWorkerError>;
}
