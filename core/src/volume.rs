use crate::error::{DzipError, Result};
use crate::reader::{ReadSeek, VolumeSource};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::path::PathBuf;

/// A volume manager that reads volumes from the filesystem using a base directory and a file list.
pub struct FileSystemVolumeManager {
    base_dir: PathBuf,
    file_list: Vec<String>,
    open_files: HashMap<u16, File>,
}

impl FileSystemVolumeManager {
    /// Creates a new FileSystemVolumeManager.
    ///
    /// # Arguments
    /// * `base_dir` - The directory containing the volume files.
    /// * `file_list` - List of filenames for auxiliary volumes (Volume 1, Volume 2, ...).
    pub fn new(base_dir: PathBuf, file_list: Vec<String>) -> Self {
        Self {
            base_dir,
            file_list,
            open_files: HashMap::new(),
        }
    }
}

impl VolumeSource for FileSystemVolumeManager {
    fn open_volume(&mut self, id: u16) -> Result<&mut dyn ReadSeek> {
        // ID 0 is reserved for the main file, which is typically handled by the DzipReader itself
        // before calling into VolumeSource for other chunks. However, if open_volume IS called with 0,
        // it implies the caller expects the manager to handle it.
        // In dzip-rs, VolumeSource is used for "Auxiliary" volumes.
        // The DzipReader usually manages the main reader.
        if id == 0 {
            return Err(DzipError::Io(std::io::Error::other(
                "Volume ID 0 is reserved for main file",
            )));
        }

        let list_index = (id - 1) as usize;
        if list_index >= self.file_list.len() {
            return Err(DzipError::VolumeNotFound(id));
        }

        match self.open_files.entry(id) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let file_name = &self.file_list[list_index];
                let path = self.base_dir.join(file_name);
                log::debug!("Opening volume {}: {}", id, path.display());
                let file =
                    File::open(&path).map_err(|e| DzipError::VolumeOpenError(id, e.to_string()))?;
                Ok(e.insert(file))
            }
        }
    }
}
