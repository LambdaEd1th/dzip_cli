use crate::Result;
use crate::error::DzipError;
use std::io::{Read, Seek, Write};
use std::path::Path;

/// Trait alias for objects that are Read + Seek + Send.
pub trait ReadSeekSend: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeekSend for T {}

/// Trait alias for objects that are Write + Seek + Send.
pub trait WriteSeekSend: Write + Seek + Send {}
impl<T: Write + Seek + Send> WriteSeekSend for T {}

/// Abstract File System Interface.
pub trait DzipFileSystem: Send + Sync {
    fn open_read(&self, path: &Path) -> Result<Box<dyn ReadSeekSend>>;

    fn create_file(&self, path: &Path) -> Result<Box<dyn WriteSeekSend>>;

    fn create_dir_all(&self, path: &Path) -> Result<()>;

    fn file_len(&self, path: &Path) -> Result<u64>;

    fn exists(&self, path: &Path) -> bool;
}

/// ---------------------------------------------------------
/// Default Implementation: Standard File System (std::fs)
/// ---------------------------------------------------------
pub struct StdFileSystem;

impl DzipFileSystem for StdFileSystem {
    fn open_read(&self, path: &Path) -> Result<Box<dyn ReadSeekSend>> {
        let f = std::fs::File::open(path)
            .map_err(|e| DzipError::IoContext(format!("Opening {:?}", path), e))?;
        Ok(Box::new(f))
    }

    fn create_file(&self, path: &Path) -> Result<Box<dyn WriteSeekSend>> {
        let f = std::fs::File::create(path)
            .map_err(|e| DzipError::IoContext(format!("Creating {:?}", path), e))?;
        Ok(Box::new(f))
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)
            .map_err(|e| DzipError::IoContext(format!("Creating dir {:?}", path), e))?;
        Ok(())
    }

    fn file_len(&self, path: &Path) -> Result<u64> {
        let meta = std::fs::metadata(path)
            .map_err(|e| DzipError::IoContext(format!("Stat {:?}", path), e))?;
        Ok(meta.len())
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
}
