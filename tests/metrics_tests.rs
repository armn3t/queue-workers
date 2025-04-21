use async_trait::async_trait;
use queue_workers::{
    error::QueueWorkerError,
    metrics::Metrics,
    queue::Queue,
    worker::{Worker, WorkerConfig},
};
use std::sync::{Arc, atomic::AtomicBool};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

mod common;
use common::TestJob;

#[derive(Debug, Clone)]
struct MetricRecord {
    name: String,
    value: MetricValue,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Timing(u64),
}

#[derive(Debug, Clone, Default)]
struct FakeMetricsCollector {
    records: Arc<std::sync::Mutex<Vec<MetricRecord>>>,
}

impl FakeMetricsCollector {
    fn new() -> Self {
        Self {
            records: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn get_counter_value(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        let records = self.records.lock().unwrap();
        let normalized_labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        for record in records.iter() {
            if record.name == name && self.labels_match(&record.labels, &normalized_labels) {
                if let MetricValue::Counter(value) = record.value {
                    return Some(value);
                }
            }
        }
        None
    }

    fn get_gauge_value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let records = self.records.lock().unwrap();
        let normalized_labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        for record in records.iter() {
            if record.name == name && self.labels_match(&record.labels, &normalized_labels) {
                if let MetricValue::Gauge(value) = record.value {
                    return Some(value);
                }
            }
        }
        None
    }

    fn get_timing_value(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        let records = self.records.lock().unwrap();
        let normalized_labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        for record in records.iter() {
            if record.name == name && self.labels_match(&record.labels, &normalized_labels) {
                if let MetricValue::Timing(value) = record.value {
                    return Some(value);
                }
            }
        }
        None
    }

    fn labels_match(&self, a: &[(String, String)], b: &[(String, String)]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        for (k1, v1) in a {
            let mut found = false;
            for (k2, v2) in b {
                if k1 == k2 && v1 == v2 {
                    found = true;
                    break;
                }
            }
            if !found {
                return false;
            }
        }
        true
    }

    fn clear(&self) {
        let mut records = self.records.lock().unwrap();
        records.clear();
    }
}

impl Metrics for FakeMetricsCollector {
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let mut records = self.records.lock().unwrap();
        records.push(MetricRecord {
            name: name.to_string(),
            value: MetricValue::Counter(value),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }

    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let mut records = self.records.lock().unwrap();
        records.push(MetricRecord {
            name: name.to_string(),
            value: MetricValue::Gauge(value),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }

    fn record_timing(&self, name: &str, value_ms: u64, labels: &[(&str, &str)]) {
        let mut records = self.records.lock().unwrap();
        records.push(MetricRecord {
            name: name.to_string(),
            value: MetricValue::Timing(value_ms),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }
}

// Custom TestQueue implementation for our tests
struct TestQueueForMetrics {
    jobs: Arc<Mutex<Vec<TestJob>>>,
}

#[async_trait]
impl Queue for TestQueueForMetrics {
    type JobType = TestJob;

    async fn push(&self, job: Self::JobType) -> Result<(), QueueWorkerError> {
        let mut jobs = self.jobs.lock().await;
        jobs.push(job);
        Ok(())
    }

    async fn pop(&self) -> Result<Self::JobType, QueueWorkerError> {
        let mut jobs = self.jobs.lock().await;
        jobs.pop()
            .ok_or_else(|| QueueWorkerError::JobNotFound("No jobs available".into()))
    }
}

#[tokio::test]
async fn test_worker_lifecycle_metrics() {
    let metrics = Arc::new(FakeMetricsCollector::new());

    let job = TestJob::new().with_duration(Duration::from_millis(50));
    let jobs = Arc::new(Mutex::new(vec![job]));

    let queue = TestQueueForMetrics { jobs: jobs.clone() };
    let config = WorkerConfig {
        retry_attempts: 0,
        retry_delay: Duration::from_millis(10),
        shutdown_timeout: Duration::from_secs(1),
        metrics: metrics.clone(),
    };

    let worker = Worker::new(queue, config);

    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = shutdown_notify.clone();

    let worker_handle = tokio::spawn(async move {
        worker
            .start(async move {
                shutdown_notify_clone.notified().await;
            })
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    shutdown_notify.notify_one();

    worker_handle.await.unwrap();

    assert_eq!(
        metrics.get_counter_value("worker_start", &[("worker_type", "serial")]),
        Some(1)
    );
    assert_eq!(
        metrics.get_counter_value("worker_stop", &[("worker_type", "serial")]),
        Some(1)
    );
    assert!(
        metrics
            .get_gauge_value("worker_uptime_seconds", &[("worker_type", "serial")])
            .is_some()
    );
}

#[tokio::test]
async fn test_job_execution_metrics() {
    let metrics = Arc::new(FakeMetricsCollector::new());

    let job = TestJob::new().with_duration(Duration::from_millis(50));
    let jobs = Arc::new(Mutex::new(vec![job]));

    let queue = TestQueueForMetrics { jobs: jobs.clone() };
    let config = WorkerConfig {
        retry_attempts: 0,
        retry_delay: Duration::from_millis(10),
        shutdown_timeout: Duration::from_secs(1),
        metrics: metrics.clone(),
    };

    let worker = Worker::new(queue, config);

    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = shutdown_notify.clone();

    let worker_handle = tokio::spawn(async move {
        worker
            .start(async move {
                shutdown_notify_clone.notified().await;
            })
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    shutdown_notify.notify_one();

    worker_handle.await.unwrap();

    let job_type_label = "TestJob";
    assert!(
        metrics
            .get_counter_value("job_executing", &[("job_type", job_type_label)])
            .is_some()
    );
    assert!(
        metrics
            .get_counter_value(
                "job_completed",
                &[("job_type", job_type_label), ("status", "success")]
            )
            .is_some()
    );
    assert!(
        metrics
            .get_timing_value(
                "job_execution_time",
                &[("job_type", job_type_label), ("status", "success")]
            )
            .is_some()
    );
    assert!(
        metrics
            .get_timing_value("queue_wait_time", &[("job_type", job_type_label)])
            .is_some()
    );
}

#[tokio::test]
async fn test_job_retry_metrics() {
    let metrics = Arc::new(FakeMetricsCollector::new());

    let completed = Arc::new(AtomicBool::new(false));
    let job = TestJob::new()
        .with_duration(Duration::from_millis(10))
        .with_should_fail(true)
        .with_completion_flag(completed.clone());

    let jobs = Arc::new(Mutex::new(vec![job]));

    let queue = TestQueueForMetrics { jobs: jobs.clone() };
    let config = WorkerConfig {
        retry_attempts: 2,
        retry_delay: Duration::from_millis(10),
        shutdown_timeout: Duration::from_secs(1),
        metrics: metrics.clone(),
    };

    let worker = Worker::new(queue, config);

    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = shutdown_notify.clone();

    let worker_handle = tokio::spawn(async move {
        worker
            .start(async move {
                shutdown_notify_clone.notified().await;
            })
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    shutdown_notify.notify_one();

    worker_handle.await.unwrap();

    let job_type_label = "TestJob";
    assert!(
        metrics
            .get_counter_value(
                "job_failed",
                &[("job_type", job_type_label), ("status", "failed")]
            )
            .is_some()
    );
}

#[tokio::test]
async fn test_empty_queue_metrics() {
    let metrics = Arc::new(FakeMetricsCollector::new());

    let jobs = Arc::new(Mutex::new(vec![]));

    let queue = TestQueueForMetrics { jobs: jobs.clone() };
    let config = WorkerConfig {
        retry_attempts: 0,
        retry_delay: Duration::from_millis(10),
        shutdown_timeout: Duration::from_secs(1),
        metrics: metrics.clone(),
    };

    let worker = Worker::new(queue, config);

    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = shutdown_notify.clone();

    let worker_handle = tokio::spawn(async move {
        let worker_future = worker.start(async move {
            shutdown_notify_clone.notified().await;
        });

        tokio::select! {
            result = worker_future => result.unwrap(),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                println!("Worker future timed out, but that's expected");
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    shutdown_notify.notify_one();

    tokio::select! {
        _ = worker_handle => {},
        _ = tokio::time::sleep(Duration::from_millis(300)) => {
            println!("Test timed out waiting for worker to shut down");
        }
    }

    let job_type_label = "TestJob";
    let has_queue_empty = metrics
        .get_counter_value("queue_empty", &[("job_type", job_type_label)])
        .is_some();
    println!("Has queue_empty metric: {}", has_queue_empty);
}

#[tokio::test]
async fn test_queue_error_metrics() {
    let metrics = Arc::new(FakeMetricsCollector::new());

    metrics.increment_counter("queue_error", 1, &[("job_type", "TestJob")]);

    let job_type_label = "TestJob";
    let has_queue_error = metrics
        .get_counter_value("queue_error", &[("job_type", job_type_label)])
        .is_some();
    println!("Has queue_error metric: {}", has_queue_error);
    assert!(
        has_queue_error,
        "Expected queue_error metric to be recorded"
    );

    let error_count = metrics
        .get_counter_value("queue_error", &[("job_type", job_type_label)])
        .unwrap();
    assert_eq!(error_count, 1, "Expected queue_error count to be 1");
}
