use dzip_core::{
    PackSink, PackSource, ReadSeekSend, Result, UnpackSink, UnpackSource, WriteSeekSend, WriteSend,
};
use std::fs::{self, File};
use std::path::{MAIN_SEPARATOR_STR, PathBuf};

// --- Unpack Implementations ---

pub struct FsUnpackSource {
    pub base_path: PathBuf,
    pub main_file_name: String,
}

impl UnpackSource for FsUnpackSource {
    fn open_main(&self) -> Result<Box<dyn ReadSeekSend>> {
        let p = self.base_path.join(&self.main_file_name);
        let f = File::open(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(Box::new(f))
    }

    fn open_split(&self, split_name: &str) -> Result<Box<dyn ReadSeekSend>> {
        let p = self.base_path.join(split_name);
        let f = File::open(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(Box::new(f))
    }

    fn get_split_len(&self, split_name: &str) -> Result<u64> {
        let p = self.base_path.join(split_name);
        let meta = fs::metadata(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(meta.len())
    }
}

pub struct FsUnpackSink {
    pub output_dir: PathBuf,
}

impl UnpackSink for FsUnpackSink {
    fn create_dir_all(&self, rel_path: &str) -> Result<()> {
        // Normalize both '/' and '\' to the OS specific separator (MAIN_SEPARATOR_STR).
        let os_rel_path = rel_path.replace(['/', '\\'], MAIN_SEPARATOR_STR);

        let p = self.output_dir.join(&os_rel_path);
        fs::create_dir_all(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(())
    }

    fn create_file(&self, rel_path: &str) -> Result<Box<dyn WriteSend>> {
        let os_rel_path = rel_path.replace(['/', '\\'], MAIN_SEPARATOR_STR);

        let p = self.output_dir.join(&os_rel_path);
        let f = File::create(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(Box::new(f))
    }
}

// --- Pack Implementations ---

pub struct FsPackSource {
    pub root_dir: PathBuf,
}

impl PackSource for FsPackSource {
    fn exists(&self, rel_path: &str) -> bool {
        self.root_dir.join(rel_path).exists()
    }

    fn open_file(&self, rel_path: &str) -> Result<Box<dyn ReadSeekSend>> {
        let p = self.root_dir.join(rel_path);
        let f = File::open(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(Box::new(f))
    }
}

pub struct FsPackSink {
    pub output_dir: PathBuf,
    pub base_name: String,
}

impl PackSink for FsPackSink {
    fn create_main(&mut self) -> Result<Box<dyn WriteSeekSend>> {
        let name = format!("{}_packed.dz", self.base_name);
        let p = self.output_dir.join(name);
        let f = File::create(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(Box::new(f))
    }

    fn create_split(&mut self, split_idx: u16) -> Result<Box<dyn WriteSeekSend>> {
        let name = format!("{}.d{:02}", self.base_name, split_idx);
        let p = self.output_dir.join(name);
        let f = File::create(&p)
            .map_err(|e| dzip_core::DzipError::IoContext(p.display().to_string(), e))?;
        Ok(Box::new(f))
    }
}
