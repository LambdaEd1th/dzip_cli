pub mod compression;
pub mod constants;
pub mod error;
pub mod pack;
pub mod types;
pub mod unpack;
pub mod utils;

pub use compression::{create_default_registry, CodecRegistry};
pub use error::DzipError;
pub use pack::do_pack;
pub use unpack::do_unpack;

/// Library-level Result type alias using DzipError.
pub type Result<T> = std::result::Result<T, DzipError>;

/// Observer trait to decouple UI/Logging from core logic.
/// Implementors must be Thread-Safe (Send + Sync).
pub trait DzipObserver: Send + Sync {
    /// Report an informational message.
    fn info(&self, message: &str);

    /// Report a warning message.
    fn warn(&self, message: &str);

    /// Start a progress bar with a total count.
    fn progress_start(&self, total_items: u64);

    /// Increment the progress bar by a specific delta.
    fn progress_inc(&self, delta: u64);

    /// Finish the progress bar with a message.
    fn progress_finish(&self, message: &str);
}

/// A no-op observer for library users who don't need feedback.
pub struct NoOpObserver;

impl DzipObserver for NoOpObserver {
    fn info(&self, _message: &str) {}
    fn warn(&self, _message: &str) {}
    fn progress_start(&self, _total: u64) {}
    fn progress_inc(&self, _delta: u64) {}
    fn progress_finish(&self, _message: &str) {}
}
