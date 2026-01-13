use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DzipError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("IO error at '{0}': {1}")]
    IoContext(String, #[source] std::io::Error),

    #[error("TOML parsing error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("Invalid magic bytes: expected 0x{0:08x}")]
    InvalidMagic(u32),

    #[error("Security error: {0}")]
    Security(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Split archive file missing: {0}")]
    SplitFileMissing(PathBuf),

    #[error("Thread execution failed: {0}")]
    ThreadPanic(String),

    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    #[error("General error: {0}")]
    Generic(String),

    #[error("Internal logic error: {0}")]
    InternalLogic(String),

    #[error("Chunk ID {0} undefined in configuration")]
    ChunkDefinitionMissing(u16),

    #[error("UI initialization error: {0}")]
    UiError(String),
}
