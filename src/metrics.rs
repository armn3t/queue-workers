pub trait Metrics: Send + Sync + 'static {
    /// Record a counter metric
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]);

    /// Record a gauge metric (value that can go up and down)
    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Record a timing metric in milliseconds
    fn record_timing(&self, name: &str, value_ms: u64, labels: &[(&str, &str)]);

    /// Record the current queue depth (number of jobs waiting)
    fn record_queue_depth(&self, queue_name: &str, depth: u64, labels: &[(&str, &str)]) {
        let mut queue_labels = Vec::with_capacity(labels.len() + 1);
        queue_labels.push(("queue", queue_name));
        queue_labels.extend_from_slice(labels);
        self.record_gauge("queue_depth", depth as f64, &queue_labels);
    }

    /// Record when a job is enqueued
    fn record_job_enqueued(&self, job_type: &str, queue_name: &str) {
        self.increment_counter(
            "job_enqueued",
            1,
            &[("job_type", job_type), ("queue", queue_name)],
        );
    }

    /// Record when a job is dequeued
    fn record_job_dequeued(&self, job_type: &str, queue_name: &str) {
        self.increment_counter(
            "job_dequeued",
            1,
            &[("job_type", job_type), ("queue", queue_name)],
        );
    }

    /// Record the time a job spent waiting in the queue before processing
    fn record_job_queue_time(&self, job_type: &str, queue_name: &str, queue_time_ms: u64) {
        self.record_timing(
            "job_queue_time",
            queue_time_ms,
            &[("job_type", job_type), ("queue", queue_name)],
        );
    }
}

#[derive(Debug, Clone, Default)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn increment_counter(&self, _name: &str, _value: u64, _labels: &[(&str, &str)]) {}
    fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn record_timing(&self, _name: &str, _value_ms: u64, _labels: &[(&str, &str)]) {}

    // Default implementations will call the base methods with no-op behavior
}
