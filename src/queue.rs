
use async_trait::async_trait;

use crate::{job::Job, error};

#[async_trait]
pub trait Queue: Send + Sync {
    type JobType: Job;


    async fn push(&self, job: Self::JobType) -> Result<(), error::QueueWorkerError>;

    async fn pop(&self) -> Result<Self::JobType, error::QueueWorkerError>;
}
