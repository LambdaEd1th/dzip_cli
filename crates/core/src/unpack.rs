use byteorder::{LittleEndian, ReadBytesExt};
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{MAIN_SEPARATOR_STR, Path, PathBuf};

use crate::compression::CodecRegistry;
use crate::constants::{
    CHUNK_LIST_TERMINATOR, CURRENT_DIR_STR, ChunkFlags, DEFAULT_BUFFER_SIZE, MAGIC,
};
use crate::error::DzipError;
use crate::io::DzipFileSystem;
use crate::types::{ArchiveMeta, ChunkDef, Config, FileEntry, RangeSettings};
use crate::utils::{decode_flags, read_null_term_string, sanitize_path};
use crate::{DzipObserver, Result};

// --- Public Types ---

/// Phase 1: Immutable Metadata
/// Represents the raw, read-only structure of a .dz archive as stored on disk.
/// It does NOT contain any calculated or corrected values (like derived chunk sizes).
#[derive(Debug)]
pub struct ArchiveMetadata {
    pub version: u8,
    pub input_path: PathBuf,
    pub user_files: Vec<String>,
    pub directories: Vec<String>,
    pub map_entries: Vec<FileMapEntry>,
    pub raw_chunks: Vec<RawChunk>, // Renamed to emphasize these are raw
    pub split_file_names: Vec<String>,
    pub range_settings: Option<RangeSettings>,
    pub main_file_len: u64,
}

/// Phase 2: Execution Plan
/// Represents a "Ready-to-Unpack" state.
/// It owns the metadata and holds the *corrected* chunk information necessary for extraction.
pub struct UnpackPlan {
    pub metadata: ArchiveMetadata,
    /// Chunks with corrected sizes (`real_c_len` calculated from offsets).
    /// The index matches `metadata.raw_chunks` and `chunk_ids`.
    pub processed_chunks: Vec<RawChunk>,
}

// Internal structures
#[derive(Debug, Clone)]
pub struct FileMapEntry {
    pub id: usize,
    pub dir_idx: usize,
    pub chunk_ids: Vec<u16>,
}

#[derive(Clone, Debug)]
pub struct RawChunk {
    pub id: u16,
    pub offset: u32,
    pub _head_c_len: u32,
    pub d_len: u32,
    pub flags: u16,
    pub file_idx: u16,
    pub real_c_len: u32, // In Metadata this is 0/raw; In Plan this is corrected.
}

// --- Main Entry Point Wrapper ---

pub fn do_unpack(
    input_path: &Path,
    out_opt: Option<PathBuf>,
    keep_raw: bool,
    registry: &CodecRegistry,
    observer: &dyn DzipObserver,
    fs_impl: &dyn DzipFileSystem,
) -> Result<()> {
    // 1. Open and Parse (Analysis Phase - Read Only)
    let meta = ArchiveMetadata::load(input_path, observer, fs_impl)?;

    // 2. Build Plan (Calculation Phase - Logic)
    // This step performs the size correction logic without mutating the original metadata structure in-place (conceptually).
    // It produces a Plan containing the corrected chunk data.
    let plan = UnpackPlan::build(meta, fs_impl)?;

    // Determine output directory
    let base_name = input_path
        .file_stem()
        .ok_or_else(|| DzipError::Generic("Invalid input file path: no stem".to_string()))?
        .to_string_lossy();
    let root_out = out_opt.unwrap_or_else(|| PathBuf::from(base_name.to_string()));

    // 3. Prepare Output Directory (Side Effect)
    fs_impl.create_dir_all(&root_out)?;

    // 4. Extract (Execution Phase - Disk I/O)
    plan.extract(&root_out, keep_raw, registry, observer, fs_impl)?;

    // 5. Save Config (Metadata Phase)
    plan.save_config(&base_name, &root_out, observer, fs_impl)?;

    Ok(())
}

// --- ArchiveMetadata Implementation ---

impl ArchiveMetadata {
    /// Loads the raw archive structure from disk.
    pub fn load(
        input_path: &Path,
        observer: &dyn DzipObserver,
        fs_impl: &dyn DzipFileSystem,
    ) -> Result<Self> {
        let main_file_raw = fs_impl.open_read(input_path)?;
        let main_file_len = fs_impl.file_len(input_path)?;
        
        let mut reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, main_file_raw);
    
        let magic = reader.read_u32::<LittleEndian>().map_err(DzipError::Io)?;
        if magic != MAGIC {
            return Err(DzipError::InvalidMagic(magic));
        }
        let num_files = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        let num_dirs = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        let version = reader.read_u8().map_err(DzipError::Io)?;
    
        observer.info(&format!(
            "Header: Ver {}, Files {}, Dirs {}",
            version, num_files, num_dirs
        ));
    
        let mut user_files = Vec::with_capacity(num_files as usize);
        for _ in 0..num_files {
            user_files.push(read_null_term_string(&mut reader).map_err(DzipError::Io)?);
        }
    
        let mut directories = Vec::with_capacity(num_dirs as usize);
        directories.push(CURRENT_DIR_STR.to_string());
        for _ in 0..(num_dirs - 1) {
            directories.push(read_null_term_string(&mut reader).map_err(DzipError::Io)?);
        }
    
        let mut map_entries = Vec::with_capacity(num_files as usize);
        for i in 0..num_files {
            let dir_id = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)? as usize;
            let mut chunk_ids = Vec::new();
            loop {
                let cid = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
                if cid == CHUNK_LIST_TERMINATOR {
                    break;
                }
                chunk_ids.push(cid);
            }
            map_entries.push(FileMapEntry {
                id: i as usize,
                dir_idx: dir_id,
                chunk_ids,
            });
        }
    
        let num_arch_files = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        let num_chunks = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        observer.info(&format!(
            "Chunk Settings: {} chunks in {} archive files",
            num_chunks, num_arch_files
        ));
    
        let mut raw_chunks = Vec::with_capacity(num_chunks as usize);
        let mut has_dz_chunk = false;
    
        for i in 0..num_chunks {
            let offset = reader.read_u32::<LittleEndian>().map_err(DzipError::Io)?;
            let c_len = reader.read_u32::<LittleEndian>().map_err(DzipError::Io)?;
            let d_len = reader.read_u32::<LittleEndian>().map_err(DzipError::Io)?;
            let flags_raw = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
            let file_idx = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
    
            let flags = ChunkFlags::from_bits_truncate(flags_raw);
            if flags.contains(ChunkFlags::DZ_RANGE) {
                has_dz_chunk = true;
            }
    
            raw_chunks.push(RawChunk {
                id: i,
                offset,
                _head_c_len: c_len,
                d_len,
                flags: flags_raw,
                file_idx,
                real_c_len: 0, // Initially 0, to be corrected in Plan
            });
        }
    
        let mut split_file_names = Vec::new();
        if num_arch_files > 1 {
            observer.info(&format!(
                "Reading {} split archive filenames...",
                num_arch_files - 1
            ));
            for _ in 0..(num_arch_files - 1) {
                split_file_names.push(read_null_term_string(&mut reader).map_err(DzipError::Io)?);
            }
        }
    
        let mut range_settings = None;
        if has_dz_chunk {
            observer.info("Detected CHUNK_DZ, reading RangeSettings...");
            range_settings = Some(RangeSettings {
                win_size: reader.read_u8().map_err(DzipError::Io)?,
                flags: reader.read_u8().map_err(DzipError::Io)?,
                offset_table_size: reader.read_u8().map_err(DzipError::Io)?,
                offset_tables: reader.read_u8().map_err(DzipError::Io)?,
                offset_contexts: reader.read_u8().map_err(DzipError::Io)?,
                ref_length_table_size: reader.read_u8().map_err(DzipError::Io)?,
                ref_length_tables: reader.read_u8().map_err(DzipError::Io)?,
                ref_offset_table_size: reader.read_u8().map_err(DzipError::Io)?,
                ref_offset_tables: reader.read_u8().map_err(DzipError::Io)?,
                big_min_match: reader.read_u8().map_err(DzipError::Io)?,
            });
        }
    
        Ok(Self {
            version,
            input_path: input_path.to_path_buf(),
            user_files,
            directories,
            map_entries,
            raw_chunks,
            split_file_names,
            range_settings,
            main_file_len,
        })
    }
}

// --- UnpackPlan Implementation ---

impl UnpackPlan {
    /// Consumes Metadata and builds a Plan by calculating corrected chunk sizes.
    pub fn build(metadata: ArchiveMetadata, fs_impl: &dyn DzipFileSystem) -> Result<Self> {
        let processed_chunks = Self::calculate_chunk_sizes(&metadata, fs_impl)?;
        Ok(Self {
            metadata,
            processed_chunks,
        })
    }

    /// Pure logic to calculate corrected sizes. Does NOT mutate metadata.
    fn calculate_chunk_sizes(meta: &ArchiveMetadata, fs_impl: &dyn DzipFileSystem) -> Result<Vec<RawChunk>> {
        let base_dir = meta.input_path.parent().unwrap_or(Path::new(CURRENT_DIR_STR));
        
        // We clone the raw chunks to create a working set for correction.
        // This preserves the original 'raw_chunks' in metadata if we ever need to inspect them.
        let mut chunks = meta.raw_chunks.clone();
        
        let mut file_chunks_map: HashMap<u16, Vec<usize>> = HashMap::new();
        for (idx, c) in chunks.iter().enumerate() {
            file_chunks_map.entry(c.file_idx).or_default().push(idx);
        }

        for (f_idx, c_indices) in file_chunks_map.iter() {
            let mut sorted_indices = c_indices.clone();
            // Sort by offset
            sorted_indices.sort_by_key(|&i| chunks[i].offset);

            let current_file_size = if *f_idx == 0 {
                meta.main_file_len
            } else {
                let idx = (*f_idx - 1) as usize;
                let split_name = meta.split_file_names.get(idx).ok_or_else(|| {
                    DzipError::Generic(format!("Invalid split file index {} in header", f_idx))
                })?;
                let split_path = base_dir.join(split_name);
                
                match fs_impl.file_len(&split_path) {
                    Ok(len) => len,
                    Err(_) => return Err(DzipError::SplitFileMissing(split_path)),
                }
            };

            for k in 0..sorted_indices.len() {
                let idx = sorted_indices[k];
                let current_offset = chunks[idx].offset;
                let next_offset = if k == sorted_indices.len() - 1 {
                    current_file_size as u32
                } else {
                    chunks[sorted_indices[k + 1]].offset
                };

                // The Correction Logic
                if next_offset < current_offset {
                    chunks[idx].real_c_len = chunks[idx]._head_c_len;
                } else {
                    chunks[idx].real_c_len = next_offset - current_offset;
                }
            }
        }
        Ok(chunks)
    }

    pub fn extract(
        &self,
        root_out: &Path,
        keep_raw: bool,
        registry: &CodecRegistry,
        observer: &dyn DzipObserver,
        fs_impl: &dyn DzipFileSystem,
    ) -> Result<()> {
        observer.info(&format!(
            "Extracting {} files to {:?}...",
            self.metadata.map_entries.len(),
            root_out
        ));
    
        let base_dir = self.metadata.input_path.parent().unwrap_or(Path::new(CURRENT_DIR_STR));
    
        // Use processed_chunks for lookup
        let chunk_indices: HashMap<u16, usize> = self
            .processed_chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (c.id, i))
            .collect();
    
        observer.progress_start(self.metadata.map_entries.len() as u64);
    
        self.metadata.map_entries.par_iter().try_for_each_init(
            HashMap::new, 
            |file_cache: &mut HashMap<u16, Box<dyn crate::io::ReadSeekSend>>, entry| -> Result<()> {
                let fname = &self.metadata.user_files[entry.id];
                let raw_dir = if entry.dir_idx < self.metadata.directories.len() {
                    &self.metadata.directories[entry.dir_idx]
                } else {
                    CURRENT_DIR_STR
                };
                let full_raw_path = if raw_dir == CURRENT_DIR_STR || raw_dir.is_empty() {
                    fname.clone()
                } else {
                    format!("{}/{}", raw_dir, fname)
                };
    
                let disk_path = sanitize_path(root_out, &full_raw_path)?;
    
                if let Some(parent) = disk_path.parent() {
                    fs_impl.create_dir_all(parent)?;
                }
                
                let out_file = fs_impl.create_file(&disk_path)?;
                let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, out_file);
    
                for cid in &entry.chunk_ids {
                    if let Some(&idx) = chunk_indices.get(cid) {
                        // Use processed_chunks (corrected sizes)
                        let chunk = &self.processed_chunks[idx];
    
                        let source_file = match file_cache.entry(chunk.file_idx) {
                            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                            std::collections::hash_map::Entry::Vacant(e) => {
                                let f = if chunk.file_idx == 0 {
                                    fs_impl.open_read(&self.metadata.input_path)?
                                } else {
                                    let split_idx = (chunk.file_idx - 1) as usize;
                                    let split_name =
                                        self.metadata.split_file_names.get(split_idx).ok_or_else(|| {
                                            DzipError::Generic(format!(
                                                "Invalid archive file index {} for chunk {}",
                                                chunk.file_idx, chunk.id
                                            ))
                                        })?;
                                    let split_path = base_dir.join(split_name);
                                    
                                    match fs_impl.open_read(&split_path) {
                                        Ok(f) => f,
                                        Err(_) => return Err(DzipError::SplitFileMissing(split_path)),
                                    }
                                };
                                e.insert(f)
                            }
                        };
    
                        source_file.seek(SeekFrom::Start(chunk.offset as u64)).map_err(DzipError::Io)?;
    
                        let mut source_reader =
                            BufReader::with_capacity(DEFAULT_BUFFER_SIZE, source_file)
                                .take(chunk.real_c_len as u64);
    
                        if let Err(e) = registry.decompress(
                            &mut source_reader,
                            &mut writer,
                            chunk.flags,
                            chunk.d_len,
                        ) {
                            if keep_raw {
                                let err_msg = e.to_string();
    
                                let mut raw_buf_reader = source_reader.into_inner();
                                raw_buf_reader.seek(SeekFrom::Start(chunk.offset as u64)).map_err(DzipError::Io)?;
                                let mut raw_take = raw_buf_reader.take(chunk.real_c_len as u64);
    
                                observer.warn(&format!(
                                    "Failed to decompress chunk {}: {}. Writing raw data (keep-raw enabled).",
                                    chunk.id, err_msg
                                ));
                                std::io::copy(&mut raw_take, &mut writer).map_err(DzipError::Io)?;
                            } else {
                                 return Err(e);
                            }
                        }
                    }
                }
                writer.flush().map_err(DzipError::Io)?;
                observer.progress_inc(1);
                Ok(())
            },
        )?;
    
        observer.progress_finish("Done");
        Ok(())
    }

    pub fn save_config(
        &self,
        base_name: &str,
        _root_out: &Path,
        observer: &dyn DzipObserver,
        fs_impl: &dyn DzipFileSystem,
    ) -> Result<()> {
        let mut toml_files = Vec::new();
        for entry in &self.metadata.map_entries {
            let fname = &self.metadata.user_files[entry.id];
            let raw_dir = if entry.dir_idx < self.metadata.directories.len() {
                &self.metadata.directories[entry.dir_idx]
            } else {
                CURRENT_DIR_STR
            };
            let full_raw_path = if raw_dir == CURRENT_DIR_STR || raw_dir.is_empty() {
                fname.clone()
            } else {
                format!("{}/{}", raw_dir, fname)
            };
            let rel_path_display = full_raw_path.replace(['/', '\\'], MAIN_SEPARATOR_STR);
            let dir_display = raw_dir.replace(['/', '\\'], MAIN_SEPARATOR_STR);
    
            toml_files.push(FileEntry {
                path: rel_path_display,
                directory: dir_display,
                filename: fname.clone(),
                chunks: entry.chunk_ids.clone(),
            });
        }
    
        let mut toml_chunks = Vec::new();
        // Use processed_chunks for config, as they have the correct sizes
        let mut sorted_chunks = self.processed_chunks.clone();
        sorted_chunks.sort_by_key(|c| c.id);
    
        for c in sorted_chunks {
            toml_chunks.push(ChunkDef {
                id: c.id,
                offset: c.offset,
                size_compressed: c.real_c_len,
                size_decompressed: c.d_len,
                flags: decode_flags(c.flags),
                archive_file_index: c.file_idx,
            });
        }
    
        let config = Config {
            archive: ArchiveMeta {
                version: self.metadata.version,
                total_files: self.metadata.map_entries.len() as u16,
                total_directories: self.metadata.directories.len() as u16,
                total_chunks: self.processed_chunks.len() as u16,
            },
            archive_files: self.metadata.split_file_names.clone(),
            range_settings: self.metadata.range_settings.clone(),
            files: toml_files,
            chunks: toml_chunks,
        };
    
        let config_path = PathBuf::from(format!("{}.toml", base_name));
    
        let toml_str = toml::to_string_pretty(&config).map_err(DzipError::TomlSer)?;
        
        let mut f = fs_impl.create_file(&config_path)?;
        f.write_all(toml_str.as_bytes()).map_err(DzipError::Io)?;
    
        observer.info(&format!("Unpack complete. Config saved to {:?}", config_path));
        Ok(())
    }
}