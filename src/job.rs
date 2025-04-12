
use std::future;
use uuid::Uuid;
use async_trait::async_trait;

#[async_trait]
pub trait Job: Send + Sync {
    type Output;
    type Error;

    async fn execute(&self) -> Result<Self::Output, Self::Error>;
}

pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
}

// TODO: add functionality for new() and updating the status
pub struct JobMetadata {
    id: Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    status: JobStatus,
}