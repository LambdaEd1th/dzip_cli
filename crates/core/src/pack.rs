use byteorder::{LittleEndian, WriteBytesExt};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};

use crate::compression::CodecRegistry;
use crate::constants::{
    CHUNK_LIST_TERMINATOR, CURRENT_DIR_STR, ChunkFlags, DEFAULT_BUFFER_SIZE, MAGIC,
};
use crate::error::DzipError;
use crate::io::{DzipFileSystem, WriteSeekSend};
use crate::types::{ChunkDef, Config};
use crate::utils::{encode_flags, normalize_path};
use crate::{DzipObserver, Result};

// --- Type Aliases ---

/// Alias for the dynamic writer object to simplify signatures.
type BoxedWriter = Box<dyn WriteSeekSend>;

/// Alias for the complex return type of the compression pipeline.
/// Returns the updated chunks definition and the main writer (handed back ownership).
type PipelineOutput = (Vec<ChunkDef>, BufWriter<BoxedWriter>);

// --- Public Types ---

/// Represents a validated and indexed packing plan.
pub struct PackPlan {
    pub config: Config,
    pub config_parent_dir: PathBuf,
    pub base_dir_name: String,

    // --- Calculated/Derived Data ---
    pub chunk_source_map: HashMap<u16, (Arc<PathBuf>, u64, usize)>,
    pub sorted_dirs: Vec<String>,
    pub dir_map: HashMap<String, usize>,
    pub has_dz_chunk: bool,
}

struct WriterContext {
    // [Modified] Use type alias
    main: BufWriter<BoxedWriter>,
    split: HashMap<u16, BufWriter<BoxedWriter>>,
}

struct CompressionJob {
    chunk_idx: usize,
    source_path: Arc<PathBuf>,
    offset: u64,
    read_len: usize,
    flags: Vec<std::borrow::Cow<'static, str>>,
}

// --- Main Entry Point Wrapper ---

pub fn do_pack(
    config_path: &PathBuf,
    registry: &CodecRegistry,
    observer: &dyn DzipObserver,
    fs_impl: &dyn DzipFileSystem,
) -> Result<()> {
    // 1. Build Plan (Validation Phase)
    let plan = PackPlan::build(config_path, observer, fs_impl)?;

    // 2. Execute Plan (Execution Phase)
    plan.execute(registry, observer, fs_impl)?;

    Ok(())
}

// --- PackPlan Implementation ---

impl PackPlan {
    /// Phase 1: Build the Plan
    pub fn build(
        config_path: &PathBuf,
        observer: &dyn DzipObserver,
        fs_impl: &dyn DzipFileSystem,
    ) -> Result<Self> {
        // A. Read and Parse Config
        let mut config_reader = fs_impl.open_read(config_path)?;
        let mut toml_content = String::new();
        config_reader
            .read_to_string(&mut toml_content)
            .map_err(|e| {
                DzipError::IoContext(format!("Failed to read config {:?}", config_path), e)
            })?;

        let config: Config = toml::from_str(&toml_content)?;

        let config_parent_dir = config_path
            .parent()
            .ok_or_else(|| {
                DzipError::Config("Cannot determine parent directory of config file".to_string())
            })?
            .to_path_buf();

        let base_dir_name = config_path
            .file_stem()
            .ok_or_else(|| DzipError::Config("Invalid config filename".to_string()))?
            .to_string_lossy()
            .to_string();

        // B. Index Source Files (Validation Logic)
        observer.info("Indexing source files...");

        let resource_root = config_parent_dir.join(&base_dir_name);

        // 1. Pre-process chunk definitions
        let mut chunk_map_def = HashMap::new();
        let mut has_dz_chunk = false;
        for c in &config.chunks {
            chunk_map_def.insert(c.id, c.clone());
            let flags = ChunkFlags::from_bits_truncate(encode_flags(&c.flags));
            if flags.contains(ChunkFlags::DZ_RANGE) {
                has_dz_chunk = true;
            }
        }

        // 2. Validate files and build chunk source map
        let mut chunk_source_map = HashMap::new();
        for f_entry in &config.files {
            let raw_path = Path::new(&f_entry.path);
            let normalized_rel = normalize_path(raw_path);
            let full_path = resource_root.join(normalized_rel);

            if !fs_impl.exists(&full_path) {
                return Err(DzipError::Config(format!(
                    "Source file not found: {:?}",
                    full_path
                )));
            }

            let full_path_arc = Arc::new(full_path);
            let mut current_offset: u64 = 0;

            for cid in &f_entry.chunks {
                let c_def = chunk_map_def
                    .get(cid)
                    .ok_or(DzipError::ChunkDefinitionMissing(*cid))?;

                let flags = ChunkFlags::from_bits_truncate(encode_flags(&c_def.flags));
                let read_len = if flags.contains(ChunkFlags::DZ_RANGE) {
                    c_def.size_compressed
                } else {
                    c_def.size_decompressed
                } as usize;

                chunk_source_map.insert(*cid, (full_path_arc.clone(), current_offset, read_len));
                current_offset += read_len as u64;
            }
        }

        // 3. Build Directory Index
        let mut unique_dirs = HashSet::new();
        for f in &config.files {
            let d = f.directory.trim();
            if d.is_empty() || d == CURRENT_DIR_STR {
                unique_dirs.insert(CURRENT_DIR_STR.to_string());
            } else {
                unique_dirs.insert(d.replace('\\', "/"));
            }
        }
        if !unique_dirs.contains(CURRENT_DIR_STR) {
            unique_dirs.insert(CURRENT_DIR_STR.to_string());
        }
        let mut sorted_dirs: Vec<String> = unique_dirs.into_iter().collect();
        sorted_dirs.sort();
        if let Some(pos) = sorted_dirs.iter().position(|x| x == CURRENT_DIR_STR) {
            sorted_dirs.remove(pos);
        }
        sorted_dirs.insert(0, CURRENT_DIR_STR.to_string());

        let dir_map: HashMap<String, usize> = sorted_dirs
            .iter()
            .enumerate()
            .map(|(i, d)| (d.clone(), i))
            .collect();

        Ok(Self {
            config,
            config_parent_dir,
            base_dir_name,
            chunk_source_map,
            sorted_dirs,
            dir_map,
            has_dz_chunk,
        })
    }

    /// Phase 2: Execute the Plan
    pub fn execute(
        &self,
        registry: &CodecRegistry,
        observer: &dyn DzipObserver,
        fs_impl: &dyn DzipFileSystem,
    ) -> Result<()> {
        observer.info(&format!("Packing from directory: {:?}", self.base_dir_name));

        let mut writers = self.prepare_writers(fs_impl)?;
        let chunk_table_start = self.build_and_write_header(&mut writers.main)?;
        let current_offset_0 = writers.main.stream_position().map_err(DzipError::Io)? as u32;

        let final_chunks = self.run_compression_pipeline(
            current_offset_0,
            writers.main,
            writers.split,
            registry,
            observer,
            fs_impl,
        )?;

        let (updated_chunks, mut main_writer_final) = final_chunks;
        Self::write_final_chunk_table(&mut main_writer_final, chunk_table_start, &updated_chunks)?;

        observer.info("All files packed successfully.");
        Ok(())
    }

    // --- Internal Helpers ---

    fn prepare_writers(&self, fs_impl: &dyn DzipFileSystem) -> Result<WriterContext> {
        let out_filename_0 = format!("{}_packed.dz", self.base_dir_name);

        let f0 = fs_impl.create_file(&self.config_parent_dir.join(out_filename_0))?;
        let writer0 = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f0);

        let mut split_writers = HashMap::new();

        for (i, fname) in self.config.archive_files.iter().enumerate() {
            let idx = (i + 1) as u16;
            let path = self.config_parent_dir.join(fname);

            let f = fs_impl.create_file(&path)?;
            split_writers.insert(idx, BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f));
        }

        Ok(WriterContext {
            main: writer0,
            split: split_writers,
        })
    }

    fn build_and_write_header(
        &self,
        writer: &mut BufWriter<BoxedWriter>, // [Modified] Use alias
    ) -> Result<u64> {
        let mut header_buffer = Cursor::new(Vec::new());
        header_buffer
            .write_u32::<LittleEndian>(MAGIC)
            .map_err(DzipError::Io)?;
        header_buffer
            .write_u16::<LittleEndian>(self.config.files.len() as u16)
            .map_err(DzipError::Io)?;
        header_buffer
            .write_u16::<LittleEndian>(self.sorted_dirs.len() as u16)
            .map_err(DzipError::Io)?;
        header_buffer.write_u8(0).map_err(DzipError::Io)?;

        for f in &self.config.files {
            header_buffer
                .write_all(f.filename.as_bytes())
                .map_err(DzipError::Io)?;
            header_buffer.write_u8(0).map_err(DzipError::Io)?;
        }
        for d in self.sorted_dirs.iter().skip(1) {
            header_buffer
                .write_all(d.replace('/', "\\").as_bytes())
                .map_err(DzipError::Io)?;
            header_buffer.write_u8(0).map_err(DzipError::Io)?;
        }
        for f in &self.config.files {
            let raw_d = f.directory.replace('\\', "/");
            let d_key = if raw_d.is_empty() || raw_d == CURRENT_DIR_STR {
                CURRENT_DIR_STR
            } else {
                &raw_d
            };
            let d_id = *self.dir_map.get(d_key).unwrap_or(&0) as u16;
            header_buffer
                .write_u16::<LittleEndian>(d_id)
                .map_err(DzipError::Io)?;
            for cid in &f.chunks {
                header_buffer
                    .write_u16::<LittleEndian>(*cid)
                    .map_err(DzipError::Io)?;
            }
            header_buffer
                .write_u16::<LittleEndian>(CHUNK_LIST_TERMINATOR)
                .map_err(DzipError::Io)?;
        }
        header_buffer
            .write_u16::<LittleEndian>((1 + self.config.archive_files.len()) as u16)
            .map_err(DzipError::Io)?;
        header_buffer
            .write_u16::<LittleEndian>(self.config.chunks.len() as u16)
            .map_err(DzipError::Io)?;

        let chunk_table_start = header_buffer.position();

        for _ in 0..self.config.chunks.len() {
            for _ in 0..16 {
                header_buffer.write_u8(0).map_err(DzipError::Io)?;
            }
        }

        if !self.config.archive_files.is_empty() {
            for fname in &self.config.archive_files {
                header_buffer
                    .write_all(fname.as_bytes())
                    .map_err(DzipError::Io)?;
                header_buffer.write_u8(0).map_err(DzipError::Io)?;
            }
        }

        if self.has_dz_chunk {
            if let Some(rs) = &self.config.range_settings {
                header_buffer.write_u8(rs.win_size).map_err(DzipError::Io)?;
                header_buffer.write_u8(rs.flags).map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.offset_table_size)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.offset_tables)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.offset_contexts)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.ref_length_table_size)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.ref_length_tables)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.ref_offset_table_size)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.ref_offset_tables)
                    .map_err(DzipError::Io)?;
                header_buffer
                    .write_u8(rs.big_min_match)
                    .map_err(DzipError::Io)?;
            } else {
                for _ in 0..10 {
                    header_buffer.write_u8(0).map_err(DzipError::Io)?;
                }
            }
        }

        writer
            .write_all(header_buffer.get_ref())
            .map_err(DzipError::Io)?;
        Ok(chunk_table_start)
    }

    fn run_compression_pipeline(
        &self,
        start_offset_0: u32,
        mut writer0: BufWriter<BoxedWriter>, // [Modified] Use alias
        mut split_writers: HashMap<u16, BufWriter<BoxedWriter>>, // [Modified] Use alias
        registry: &CodecRegistry,
        observer: &dyn DzipObserver,
        fs_impl: &dyn DzipFileSystem,
    ) -> Result<PipelineOutput> {
        // [Modified] Use alias
        // Clone definitions for mutation during compression
        let mut sorted_chunks_def = self.config.chunks.clone();
        sorted_chunks_def.sort_by_key(|c| c.id);

        observer.info(&format!(
            "Compressing {} chunks ...",
            sorted_chunks_def.len()
        ));

        observer.progress_start(sorted_chunks_def.len() as u64);

        let jobs: Result<Vec<CompressionJob>> = sorted_chunks_def
            .iter()
            .enumerate()
            .map(|(i, c_def)| {
                let (source_path, src_offset, read_len) =
                    self.chunk_source_map.get(&c_def.id).ok_or_else(|| {
                        DzipError::Config(format!("Source map missing for chunk ID {}", c_def.id))
                    })?;
                Ok(CompressionJob {
                    chunk_idx: i,
                    source_path: source_path.clone(),
                    offset: *src_offset,
                    read_len: *read_len,
                    flags: c_def.flags.clone(),
                })
            })
            .collect();
        let jobs = jobs?;

        let channel_bound = rayon::current_num_threads() * 4;
        let (tx, rx) = mpsc::sync_channel::<(usize, Result<Vec<u8>>)>(channel_bound);

        let mut current_offset_0 = start_offset_0;

        let mut split_offsets_owned: HashMap<u16, u32> =
            split_writers.keys().map(|k| (*k, 0)).collect();

        std::thread::scope(|s| {
            let writer_handle = s.spawn(move || -> Result<PipelineOutput> {
                let total_chunks = sorted_chunks_def.len();
                let mut buffer: HashMap<usize, Vec<u8>> = HashMap::new();
                let mut next_idx = 0;

                while next_idx < total_chunks {
                    let data = if let Some(d) = buffer.remove(&next_idx) {
                        d
                    } else {
                        match rx.recv() {
                            Ok((idx, res)) => {
                                let chunk_data = res?;
                                if idx == next_idx {
                                    chunk_data
                                } else {
                                    buffer.insert(idx, chunk_data);
                                    continue;
                                }
                            }
                            Err(_) => {
                                return Err(DzipError::ThreadPanic(
                                    "Compression threads disconnected unexpectedly".into(),
                                ));
                            }
                        }
                    };

                    let c_def = &mut sorted_chunks_def[next_idx];

                    let target_writer = if c_def.archive_file_index == 0 {
                        &mut writer0
                    } else {
                        split_writers
                            .get_mut(&c_def.archive_file_index)
                            .ok_or_else(|| {
                                DzipError::Config(format!(
                                    "Invalid archive_file_index {}",
                                    c_def.archive_file_index
                                ))
                            })?
                    };

                    let current_pos = if c_def.archive_file_index == 0 {
                        current_offset_0
                    } else {
                        *split_offsets_owned
                            .get(&c_def.archive_file_index)
                            .unwrap_or(&0)
                    };

                    target_writer.write_all(&data).map_err(DzipError::Io)?;

                    c_def.offset = current_pos;
                    c_def.size_compressed = data.len() as u32;

                    if c_def.archive_file_index == 0 {
                        current_offset_0 += c_def.size_compressed;
                    } else {
                        *split_offsets_owned
                            .get_mut(&c_def.archive_file_index)
                            .ok_or_else(|| {
                                DzipError::InternalLogic(format!(
                                    "Split offset missing for archive index {}",
                                    c_def.archive_file_index
                                ))
                            })? += c_def.size_compressed;
                    }
                    next_idx += 1;
                    observer.progress_inc(1);
                }

                observer.progress_finish("Done");

                for w in split_writers.values_mut() {
                    w.flush().map_err(DzipError::Io)?;
                }

                Ok((sorted_chunks_def, writer0))
            });

            jobs.par_iter().for_each_with(tx, |s, job| {
                let res = (|| -> Result<Vec<u8>> {
                    let mut f_in = fs_impl.open_read(job.source_path.as_ref())?;

                    f_in.seek(SeekFrom::Start(job.offset))
                        .map_err(DzipError::Io)?;
                    let mut chunk_reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, f_in)
                        .take(job.read_len as u64);
                    let mut compressed_buffer = Vec::new();
                    let flags_int = encode_flags(&job.flags);

                    registry.compress(&mut chunk_reader, &mut compressed_buffer, flags_int)?;

                    Ok(compressed_buffer)
                })();
                let _ = s.send((job.chunk_idx, res));
            });

            writer_handle
                .join()
                .map_err(|e| DzipError::ThreadPanic(format!("{:?}", e)))?
        })
    }

    fn write_final_chunk_table(
        writer: &mut BufWriter<BoxedWriter>, // [Modified] Use alias
        table_start_pos: u64,
        chunks: &[ChunkDef],
    ) -> Result<()> {
        let mut table_buffer = Cursor::new(Vec::new());
        for c in chunks {
            table_buffer
                .write_u32::<LittleEndian>(c.offset)
                .map_err(DzipError::Io)?;
            table_buffer
                .write_u32::<LittleEndian>(c.size_compressed)
                .map_err(DzipError::Io)?;
            table_buffer
                .write_u32::<LittleEndian>(c.size_decompressed)
                .map_err(DzipError::Io)?;
            table_buffer
                .write_u16::<LittleEndian>(encode_flags(&c.flags))
                .map_err(DzipError::Io)?;
            table_buffer
                .write_u16::<LittleEndian>(c.archive_file_index)
                .map_err(DzipError::Io)?;
        }

        writer
            .seek(SeekFrom::Start(table_start_pos))
            .map_err(DzipError::Io)?;
        writer
            .write_all(table_buffer.get_ref())
            .map_err(DzipError::Io)?;
        writer.flush().map_err(DzipError::Io)?;
        Ok(())
    }
}
