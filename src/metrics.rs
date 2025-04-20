pub trait Metrics: Send + Sync + 'static {
    /// Record a counter metric
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]);

    /// Record a gauge metric (value that can go up and down)
    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Record a timing metric in milliseconds
    fn record_timing(&self, name: &str, value_ms: u64, labels: &[(&str, &str)]);
}

#[derive(Debug, Clone, Default)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn increment_counter(&self, _name: &str, _value: u64, _labels: &[(&str, &str)]) {}
    fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn record_timing(&self, _name: &str, _value_ms: u64, _labels: &[(&str, &str)]) {}
}
