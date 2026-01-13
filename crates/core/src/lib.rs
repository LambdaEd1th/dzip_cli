pub mod compression;
pub mod constants;
pub mod error;
pub mod io;
pub mod list;
pub mod pack;
pub mod types;
pub mod unpack;
pub mod utils; // [Added] Register the list module

pub use compression::{CodecRegistry, create_default_registry};
pub use error::DzipError;
pub use io::{DzipFileSystem, StdFileSystem};
pub use list::{ListEntry, do_list}; // [Added] Export list functionality
pub use pack::do_pack;
pub use unpack::do_unpack;

// --- Common Types ---

pub type Result<T> = std::result::Result<T, DzipError>;

pub trait DzipObserver: Send + Sync {
    fn info(&self, message: &str);
    fn warn(&self, message: &str);
    fn progress_start(&self, total_items: u64);
    fn progress_inc(&self, delta: u64);
    fn progress_finish(&self, message: &str);
}

pub struct NoOpObserver;

impl DzipObserver for NoOpObserver {
    fn info(&self, _message: &str) {}
    fn warn(&self, _message: &str) {}
    fn progress_start(&self, _total: u64) {}
    fn progress_inc(&self, _delta: u64) {}
    fn progress_finish(&self, _message: &str) {}
}
