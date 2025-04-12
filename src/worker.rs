
use std::future;

use crate::error::QueueWorkerError;

pub trait Worker {
    fn start(&self) -> impl future::Future<Output = Result<(), QueueWorkerError>> + Send;
    fn stop(&self) -> impl future::Future<Output = Result<(), QueueWorkerError>> + Send;

    fn status(&self) -> String;

}
