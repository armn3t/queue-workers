
use std::future;
use uuid::Uuid;

pub trait Job: Send + Sync {
    type Output;
    type Error;

    fn execute(&self) -> impl future::Future<Output = Result<Self::Output, Self::Error>> + Send;
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