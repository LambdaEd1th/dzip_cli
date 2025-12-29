pub mod compression;
pub mod constants;
pub mod error;
pub mod pack;
pub mod types;
pub mod unpack;
pub mod utils;

pub use compression::{CodecRegistry, create_default_registry};
pub use error::DzipError;
pub use pack::do_pack;
pub use unpack::do_unpack;

/// Library-level Result type alias
pub type Result<T> = std::result::Result<T, DzipError>;
