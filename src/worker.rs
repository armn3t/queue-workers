
use std::future;
use async_trait::async_trait;

use crate::error::QueueWorkerError;

#[async_trait]
pub trait Worker {
    async fn start(&self) -> Result<(), QueueWorkerError>;
    async fn stop(&self) -> Result<(), QueueWorkerError>;

    fn status(&self) -> String;

}
