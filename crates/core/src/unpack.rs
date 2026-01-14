use byteorder::{LittleEndian, ReadBytesExt};
use log::{info, warn};
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::Result;
use crate::codecs::decompress;
use crate::error::DzipError;
use crate::format::{
    CHUNK_LIST_TERMINATOR, CURRENT_DIR_STR, ChunkFlags, DEFAULT_BUFFER_SIZE, MAGIC,
};
use crate::io::{ReadSeekSend, UnpackSink, UnpackSource};
use crate::model::{ArchiveMeta, ChunkDef, Config, FileEntry, RangeSettings};
use crate::utils::{decode_flags, read_null_term_string};

// --- Structures ---

#[derive(Debug)]
pub struct ArchiveMetadata {
    pub version: u8,
    pub user_files: Vec<String>,
    pub directories: Vec<String>,
    pub map_entries: Vec<FileMapEntry>,
    pub raw_chunks: Vec<RawChunk>,
    pub split_file_names: Vec<String>,
    pub range_settings: Option<RangeSettings>,
    pub main_file_len: u64,
}

pub struct UnpackPlan {
    pub metadata: ArchiveMetadata,
    pub processed_chunks: Vec<RawChunk>,
}

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
    pub real_c_len: u32,
}

// --- Wrapper ---

/// Main entry point for unpacking.
/// Uses `&dyn UnpackSink` to allow parallel file creation (thread-safe).
pub fn do_unpack(
    source: &dyn UnpackSource,
    sink: &dyn UnpackSink,
    keep_raw: bool,
) -> Result<Config> {
    let meta = ArchiveMetadata::load(source)?;
    let plan = UnpackPlan::build(meta, source)?;
    plan.extract(sink, keep_raw, source)?;
    let config = plan.generate_config_struct()?;
    info!("Unpack complete. Config object generated.");
    Ok(config)
}

// --- Implementations ---

impl ArchiveMetadata {
    /// Loads the archive metadata from the main source file.
    /// This method has been refactored to use smaller helper functions for better readability.
    pub fn load(source: &dyn UnpackSource) -> Result<Self> {
        let mut main_file_raw = source.open_main()?;
        let main_file_len = main_file_raw
            .seek(SeekFrom::End(0))
            .map_err(DzipError::Io)?;
        main_file_raw
            .seek(SeekFrom::Start(0))
            .map_err(DzipError::Io)?;

        let mut reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, main_file_raw);

        // 1. Read Basic Header
        let (version, num_files, num_dirs) = Self::read_header_basic(&mut reader)?;

        // 2. Read File Names
        let user_files = Self::read_file_names(&mut reader, num_files)?;

        // 3. Read Directories
        let directories = Self::read_directories(&mut reader, num_dirs)?;

        // 4. Read File-Chunk Maps
        let map_entries = Self::read_file_maps(&mut reader, num_files)?;

        // 5. Read Chunk Table
        let (raw_chunks, num_arch_files, has_dz_chunk) = Self::read_chunk_table(&mut reader)?;

        // 6. Read Split Filenames (if any)
        let split_file_names = Self::read_split_filenames(&mut reader, num_arch_files)?;

        // 7. Read Range Settings (if needed)
        let range_settings = Self::read_range_settings(&mut reader, has_dz_chunk)?;

        Ok(Self {
            version,
            user_files,
            directories,
            map_entries,
            raw_chunks,
            split_file_names,
            range_settings,
            main_file_len,
        })
    }

    // --- Helper Methods for Loading ---

    fn read_header_basic<R: Read>(reader: &mut R) -> Result<(u8, u16, u16)> {
        let magic = reader.read_u32::<LittleEndian>().map_err(DzipError::Io)?;
        if magic != MAGIC {
            return Err(DzipError::InvalidMagic(magic));
        }
        let num_files = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        let num_dirs = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        let version = reader.read_u8().map_err(DzipError::Io)?;

        info!(
            "Header: Ver {}, Files {}, Dirs {}",
            version, num_files, num_dirs
        );
        Ok((version, num_files, num_dirs))
    }

    fn read_file_names<R: std::io::BufRead>(reader: &mut R, count: u16) -> Result<Vec<String>> {
        let mut files = Vec::with_capacity(count as usize);
        for _ in 0..count {
            files.push(read_null_term_string(reader).map_err(DzipError::Io)?);
        }
        Ok(files)
    }

    fn read_directories<R: std::io::BufRead>(reader: &mut R, count: u16) -> Result<Vec<String>> {
        let mut dirs = Vec::with_capacity(count as usize);
        // The first directory is always logically current dir "."
        dirs.push(CURRENT_DIR_STR.to_string());
        for _ in 0..(count - 1) {
            // We read the raw string as-is (e.g., "textures\ui").
            // The CLI/Sink is responsible for OS adaptation.
            dirs.push(read_null_term_string(reader).map_err(DzipError::Io)?);
        }
        Ok(dirs)
    }

    fn read_file_maps<R: Read>(reader: &mut R, count: u16) -> Result<Vec<FileMapEntry>> {
        let mut entries = Vec::with_capacity(count as usize);
        for i in 0..count {
            let dir_id = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)? as usize;
            let mut chunk_ids = Vec::new();
            loop {
                let cid = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
                if cid == CHUNK_LIST_TERMINATOR {
                    break;
                }
                chunk_ids.push(cid);
            }
            entries.push(FileMapEntry {
                id: i as usize,
                dir_idx: dir_id,
                chunk_ids,
            });
        }
        Ok(entries)
    }

    fn read_chunk_table<R: Read>(reader: &mut R) -> Result<(Vec<RawChunk>, u16, bool)> {
        let num_arch_files = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        let num_chunks = reader.read_u16::<LittleEndian>().map_err(DzipError::Io)?;
        info!(
            "Chunk Settings: {} chunks in {} archive files",
            num_chunks, num_arch_files
        );

        let mut chunks = Vec::with_capacity(num_chunks as usize);
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

            chunks.push(RawChunk {
                id: i,
                offset,
                _head_c_len: c_len,
                d_len,
                flags: flags_raw,
                file_idx,
                real_c_len: 0,
            });
        }

        Ok((chunks, num_arch_files, has_dz_chunk))
    }

    fn read_split_filenames<R: std::io::BufRead>(
        reader: &mut R,
        num_arch_files: u16,
    ) -> Result<Vec<String>> {
        let mut names = Vec::new();
        if num_arch_files > 1 {
            info!("Reading {} split archive filenames...", num_arch_files - 1);
            for _ in 0..(num_arch_files - 1) {
                names.push(read_null_term_string(reader).map_err(DzipError::Io)?);
            }
        }
        Ok(names)
    }

    fn read_range_settings<R: Read>(
        reader: &mut R,
        has_dz_chunk: bool,
    ) -> Result<Option<RangeSettings>> {
        if has_dz_chunk {
            info!("Detected CHUNK_DZ, reading RangeSettings...");
            let rs = RangeSettings {
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
            };
            Ok(Some(rs))
        } else {
            Ok(None)
        }
    }
}

impl UnpackPlan {
    pub fn build(metadata: ArchiveMetadata, source: &dyn UnpackSource) -> Result<Self> {
        let processed_chunks = Self::calculate_chunk_sizes(&metadata, source)?;
        Ok(Self {
            metadata,
            processed_chunks,
        })
    }

    fn calculate_chunk_sizes(
        meta: &ArchiveMetadata,
        source: &dyn UnpackSource,
    ) -> Result<Vec<RawChunk>> {
        let mut chunks = meta.raw_chunks.clone();
        let mut file_chunks_map: HashMap<u16, Vec<usize>> = HashMap::new();
        for (idx, c) in chunks.iter().enumerate() {
            file_chunks_map.entry(c.file_idx).or_default().push(idx);
        }

        for (f_idx, c_indices) in file_chunks_map.iter() {
            let mut sorted_indices = c_indices.clone();
            sorted_indices.sort_by_key(|&i| chunks[i].offset);
            let current_file_size = if *f_idx == 0 {
                meta.main_file_len
            } else {
                let idx = (*f_idx - 1) as usize;
                let split_name = meta.split_file_names.get(idx).ok_or_else(|| {
                    DzipError::Generic(format!("Invalid split file index {} in header", f_idx))
                })?;
                source.get_split_len(split_name)?
            };

            for k in 0..sorted_indices.len() {
                let idx = sorted_indices[k];
                let current_offset = chunks[idx].offset;
                let next_offset = if k == sorted_indices.len() - 1 {
                    current_file_size as u32
                } else {
                    chunks[sorted_indices[k + 1]].offset
                };
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
        sink: &dyn UnpackSink,
        keep_raw: bool,
        source: &dyn UnpackSource,
    ) -> Result<()> {
        info!("Extracting {} files...", self.metadata.map_entries.len());
        let chunk_indices: HashMap<u16, usize> = self
            .processed_chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (c.id, i))
            .collect();

        self.metadata.map_entries.par_iter().try_for_each_init(
            HashMap::new,
            |file_cache: &mut HashMap<u16, Box<dyn ReadSeekSend>>, entry| -> Result<()> {
                let fname = &self.metadata.user_files[entry.id];
                let raw_dir = if entry.dir_idx < self.metadata.directories.len() {
                    &self.metadata.directories[entry.dir_idx]
                } else {
                    CURRENT_DIR_STR
                };

                // [Refactor] Use PathBuf for robust path construction
                let mut path_buf = PathBuf::from(raw_dir);
                if raw_dir != CURRENT_DIR_STR && !raw_dir.is_empty() {
                    path_buf.push(fname);
                } else {
                    path_buf = PathBuf::from(fname);
                }

                // Convert back to string for the Sink interface (which expects relative path string)
                // We use to_string_lossy() to handle potential non-UTF8 paths gracefully,
                // though the archive format enforces UTF-8 strings generally.
                let rel_path = path_buf.to_string_lossy().replace('\\', "/");

                // [Refactor] Fix Clippy collapsible_if warning using Option::filter
                if let Some(parent) = path_buf
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty() && p.as_os_str() != ".")
                {
                    sink.create_dir_all(&parent.to_string_lossy())?;
                }

                let out_file = sink.create_file(&rel_path)?;
                let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, out_file);

                for cid in &entry.chunk_ids {
                    if let Some(&idx) = chunk_indices.get(cid) {
                        let chunk = &self.processed_chunks[idx];
                        let source_file = match file_cache.entry(chunk.file_idx) {
                            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                            std::collections::hash_map::Entry::Vacant(e) => {
                                let f = if chunk.file_idx == 0 {
                                    source.open_main()?
                                } else {
                                    let split_idx = (chunk.file_idx - 1) as usize;
                                    let split_name =
                                        self.metadata.split_file_names.get(split_idx).ok_or_else(
                                            || {
                                                DzipError::Generic(format!(
                                                    "Invalid archive file index {} for chunk {}",
                                                    chunk.file_idx, chunk.id
                                                ))
                                            },
                                        )?;
                                    source.open_split(split_name)?
                                };
                                e.insert(f)
                            }
                        };
                        source_file
                            .seek(SeekFrom::Start(chunk.offset as u64))
                            .map_err(DzipError::Io)?;
                        let mut source_reader =
                            BufReader::with_capacity(DEFAULT_BUFFER_SIZE, source_file)
                                .take(chunk.real_c_len as u64);
                        if let Err(e) =
                            decompress(&mut source_reader, &mut writer, chunk.flags, chunk.d_len)
                        {
                            if keep_raw {
                                let err_msg = e.to_string();
                                let mut raw_buf_reader = source_reader.into_inner();
                                raw_buf_reader
                                    .seek(SeekFrom::Start(chunk.offset as u64))
                                    .map_err(DzipError::Io)?;
                                let mut raw_take = raw_buf_reader.take(chunk.real_c_len as u64);
                                warn!(
                                    "Failed to decompress chunk {}: {}. Writing raw data.",
                                    chunk.id, err_msg
                                );
                                std::io::copy(&mut raw_take, &mut writer).map_err(DzipError::Io)?;
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                writer.flush().map_err(DzipError::Io)?;
                Ok(())
            },
        )?;
        Ok(())
    }

    pub fn generate_config_struct(&self) -> Result<Config> {
        let mut toml_files = Vec::new();

        for entry in &self.metadata.map_entries {
            let fname = &self.metadata.user_files[entry.id];
            let raw_dir = if entry.dir_idx < self.metadata.directories.len() {
                &self.metadata.directories[entry.dir_idx]
            } else {
                CURRENT_DIR_STR
            };

            let mut path_buf = PathBuf::from(raw_dir);
            if raw_dir != CURRENT_DIR_STR && !raw_dir.is_empty() {
                path_buf.push(fname);
            } else {
                path_buf = PathBuf::from(fname);
            }

            // Normalize separators to forward slash for the TOML config standard
            let full_raw_path = path_buf.to_string_lossy().replace('\\', "/");

            toml_files.push(FileEntry {
                path: full_raw_path,
                directory: raw_dir.to_string(),
                filename: fname.clone(),
                chunks: entry.chunk_ids.clone(),
            });
        }

        let mut toml_chunks = Vec::new();
        let mut sorted_chunks = self.processed_chunks.clone();
        sorted_chunks.sort_by_key(|c| c.id);

        for c in sorted_chunks {
            let flags_list = decode_flags(c.flags)
                .into_iter()
                .map(|s| s.into_owned())
                .collect();
            toml_chunks.push(ChunkDef {
                id: c.id,
                offset: c.offset,
                size_compressed: c.real_c_len,
                size_decompressed: c.d_len,
                flags: flags_list,
                archive_file_index: c.file_idx,
            });
        }

        Ok(Config {
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
        })
    }
}
