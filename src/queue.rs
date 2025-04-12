
use std::future;

use crate::{job::Job, error};

pub trait Queue: Send + Sync {
    type JobType: Job;


    fn push(&self, job: Self::JobType) -> impl future::Future<Output = Result<(), error::QueueWorkerError>> + Send;

    fn pop(&self) -> impl future::Future<Output = Result<Self::JobType, error::QueueWorkerError>> + Send;
}
