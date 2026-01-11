use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[async_trait]
pub trait Job: Send + Sync {
    type Output;
    type Error;

    /// Returns the job type identifier, used for metrics and logging.
    /// This is a required method that must be implemented for all jobs.
    /// The job type is used for metrics, logging, and serialization/deserialization.
    fn job_type(&self) -> &'static str;

    async fn execute(&self) -> Result<Self::Output, Self::Error>;

    fn should_retry(&self, _error: &Self::Error, _attempt: u32) -> bool {
        true
    }
}

/// A wrapper struct for jobs that includes the job type and enqueue time.
/// This is used for serialization/deserialization to ensure the job type is preserved
/// and to track how long jobs spend in the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobWrapper<J> {
    /// The job type identifier
    pub job_type: String,
    /// The serialized job data
    pub job_data: J,
    /// When the job was enqueued (UTC timestamp in milliseconds)
    #[serde(default = "current_time_millis")]
    pub enqueued_at: u64,
}

/// Get current time in milliseconds since UNIX epoch
pub fn current_time_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl<J> JobWrapper<J>
where
    J: Job + Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new JobWrapper from a job
    pub fn new(job: J) -> Self {
        Self {
            job_type: job.job_type().to_string(),
            job_data: job,
            enqueued_at: current_time_millis(),
        }
    }

    /// Get the job type
    pub fn job_type(&self) -> &str {
        &self.job_type
    }

    /// Get the job data
    pub fn job_data(&self) -> &J {
        &self.job_data
    }

    /// Unwrap the job wrapper and return the job data
    pub fn into_job_data(self) -> J {
        self.job_data
    }

    /// Get the time the job was enqueued
    pub fn enqueued_at(&self) -> u64 {
        self.enqueued_at
    }

    /// Calculate how long the job has been in the queue (in milliseconds)
    pub fn queue_time_ms(&self) -> u64 {
        current_time_millis().saturating_sub(self.enqueued_at)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetadata<T>
where
    T: Serialize,
{
    id: Uuid,
    payload: T,
    created_at: chrono::DateTime<chrono::Utc>,
    status: JobStatus,
}

impl<T> JobMetadata<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub fn new(payload: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            payload,
            created_at: chrono::Utc::now(),
            status: JobStatus::Pending,
        }
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn payload_mut(&mut self) -> &mut T {
        &mut self.payload
    }

    pub fn update_status(&mut self, status: JobStatus) {
        self.status = status;
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.created_at
    }

    pub fn status(&self) -> &JobStatus {
        &self.status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPayload {
        data: String,
    }

    #[test]
    fn test_new_job_metadata() {
        let metadata = JobMetadata::new(TestPayload {
            data: "test".to_string(),
        });

        assert!(!metadata.id().is_nil());

        match metadata.status() {
            JobStatus::Pending => (),
            _ => panic!("New job should have Pending status"),
        }

        let now = chrono::Utc::now();
        let diff = now - metadata.created_at();
        assert!(diff.num_milliseconds() >= 0);
        assert!(diff.num_seconds() < 1);
    }

    #[test]
    fn test_update_status() {
        let mut metadata = JobMetadata::new(TestPayload {
            data: "test".to_string(),
        });

        metadata.update_status(JobStatus::Running);
        match metadata.status() {
            JobStatus::Running => (),
            _ => panic!("Status should be Running"),
        }

        metadata.update_status(JobStatus::Completed);
        match metadata.status() {
            JobStatus::Completed => (),
            _ => panic!("Status should be Completed"),
        }

        metadata.update_status(JobStatus::Failed("Test error".to_string()));
        match metadata.status() {
            JobStatus::Failed(msg) => assert_eq!(msg, "Test error"),
            _ => panic!("Status should be Failed"),
        }
    }

    #[test]
    fn test_clone() {
        let original = JobMetadata::new(TestPayload {
            data: "test".to_string(),
        });
        let cloned = original.clone();

        assert_eq!(original.id(), cloned.id());
        assert_eq!(original.created_at(), cloned.created_at());

        match (original.status(), cloned.status()) {
            (JobStatus::Pending, JobStatus::Pending) => (),
            _ => panic!("Cloned status should match original"),
        }
    }

    #[test]
    fn test_serialization() {
        let metadata = JobMetadata::new(TestPayload {
            data: "test".to_string(),
        });

        let serialized = serde_json::to_string(&metadata).expect("Failed to serialize");

        let deserialized: JobMetadata<TestPayload> =
            serde_json::from_str(&serialized).expect("Failed to deserialize");

        assert_eq!(metadata.id(), deserialized.id());
        assert_eq!(metadata.created_at(), deserialized.created_at());

        match (metadata.status(), deserialized.status()) {
            (JobStatus::Pending, JobStatus::Pending) => (),
            _ => panic!("Deserialized status should match original"),
        }
    }
}
