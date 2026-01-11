pub mod concurrent_worker;
pub mod error;
pub mod job;
pub mod metrics;
pub mod queue;
pub mod simple_queue;
pub mod worker;

// Re-export redis_queue as simple_queue for backward compatibility
#[deprecated(since = "0.6.0", note = "Use simple_queue instead")]
pub mod redis_queue {
    pub use crate::simple_queue::*;
}
