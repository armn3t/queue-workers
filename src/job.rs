use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[async_trait]
pub trait Job: Send + Sync {
    type Output;
    type Error;

    async fn execute(&self) -> Result<Self::Output, Self::Error>;

    fn should_retry(&self, _error: &Self::Error, _attempt: u32) -> bool {
        true
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
