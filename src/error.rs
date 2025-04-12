use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueueWorkerError {
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("Job not found: {0}")]
    JobNotFound(String),
    
    #[error("Queue not found: {0}")]
    QueueNotFound(String),
    
    #[error("Invalid job data: {0}")]
    InvalidJobData(String),
    
    #[error("Tenant capacity exceeded: {0}")]
    TenantCapacityExceeded(String),
    
    #[error("Worker error: {0}")]
    WorkerError(String),
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Timeout error")]
    TimeoutError,
    
    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, QueueWorkerError>;
