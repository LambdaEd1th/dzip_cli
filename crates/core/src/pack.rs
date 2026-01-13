use byteorder::{LittleEndian, WriteBytesExt};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};

use crate::compression::CodecRegistry;
use crate::constants::{
    ChunkFlags, CHUNK_LIST_TERMINATOR, CURRENT_DIR_STR, DEFAULT_BUFFER_SIZE, MAGIC,
};
use crate::error::DzipError;
use crate::types::{ChunkDef, Config};
use crate::utils::{encode_flags, normalize_path};
use crate::{DzipObserver, Result};

// --- Internal Structures ---

struct PackContext {
    chunk_source_map: HashMap<u16, (Arc<PathBuf>, u64, usize)>,
    sorted_dirs: Vec<String>,
    dir_map: HashMap<String, usize>,
    has_dz_chunk: bool,
    base_dir_name: String,
    current_offset_0: u32,
}

struct WriterContext {
    main: BufWriter<File>,
    split: HashMap<u16, BufWriter<File>>,
}

struct CompressionJob {
    chunk_idx: usize,
    source_path: Arc<PathBuf>,
    offset: u64,
    read_len: usize,
    flags: Vec<std::borrow::Cow<'static, str>>,
}

// --- Main Entry Point ---

pub fn do_pack(
    config_path: &PathBuf,
    registry: &CodecRegistry,
    observer: &dyn DzipObserver,
) -> Result<()> {
    // Read and parse config with context
    let toml_content = fs::read_to_string(config_path).map_err(|e| {
        DzipError::IoContext(format!("Failed to read config file {:?}", config_path), e)
    })?;
    let config: Config = toml::from_str(&toml_content)?;

    let config_parent_dir = config_path.parent().ok_or_else(|| {
        DzipError::Config("Cannot determine parent directory of config file".to_string())
    })?;

    // 1. Index files
    observer.info("Indexing source files...");
    let mut ctx = index_source_files(&config, config_path, config_parent_dir)?;

    // 2. Prepare output file Writers
    observer.info(&format!("Packing from directory: {:?}", ctx.base_dir_name));
    let mut writers = prepare_writers(&config, config_parent_dir, &ctx.base_dir_name)?;

    // 3. Build and write file header
    let chunk_table_start = build_and_write_header(&config, &ctx, &mut writers.main)?;
    ctx.current_offset_0 = writers.main.stream_position().map_err(DzipError::Io)? as u32;

    // 4. Execute compression pipeline
    let final_chunks = run_compression_pipeline(
        config.chunks.clone(),
        &ctx,
        writers.main,
        writers.split,
        registry,
        observer,
    )?;

    // 5. Write back the final Chunk table
    let (updated_chunks, mut main_writer_final) = final_chunks;

    write_final_chunk_table(&mut main_writer_final, chunk_table_start, &updated_chunks)?;

    observer.info("All files packed successfully.");
    Ok(())
}

// --- Sub-functions ---

fn index_source_files(
    config: &Config,
    config_path: &Path,
    base_path: &Path,
) -> Result<PackContext> {
    let base_dir_name = config_path
        .file_stem()
        .ok_or_else(|| DzipError::Config("Invalid config filename".to_string()))?
        .to_string_lossy()
        .to_string();

    let resource_root = base_path.join(&base_dir_name);

    let mut chunk_map_def = HashMap::new();
    let mut has_dz_chunk = false;
    for c in &config.chunks {
        chunk_map_def.insert(c.id, c.clone());
        let flags = ChunkFlags::from_bits_truncate(encode_flags(&c.flags));
        if flags.contains(ChunkFlags::DZ_RANGE) {
            has_dz_chunk = true;
        }
    }

    let mut chunk_source_map = HashMap::new();
    for f_entry in &config.files {
        let raw_path = Path::new(&f_entry.path);
        let normalized_rel = normalize_path(raw_path);
        let full_path = resource_root.join(normalized_rel);

        if !full_path.exists() {
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

    Ok(PackContext {
        chunk_source_map,
        sorted_dirs,
        dir_map,
        has_dz_chunk,
        base_dir_name,
        current_offset_0: 0,
    })
}

fn prepare_writers(
    config: &Config,
    base_path: &Path,
    base_filename: &str,
) -> Result<WriterContext> {
    let out_filename_0 = format!("{}_packed.dz", base_filename);
    let f0 = File::create(base_path.join(out_filename_0)).map_err(DzipError::Io)?;
    let writer0 = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f0);

    let mut split_writers = HashMap::new();

    for (i, fname) in config.archive_files.iter().enumerate() {
        let idx = (i + 1) as u16;
        let path = base_path.join(fname);
        let f = File::create(&path).map_err(DzipError::Io)?;
        split_writers.insert(idx, BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f));
    }

    Ok(WriterContext {
        main: writer0,
        split: split_writers,
    })
}

fn build_and_write_header(
    config: &Config,
    ctx: &PackContext,
    writer: &mut BufWriter<File>,
) -> Result<u64> {
    let mut header_buffer = Cursor::new(Vec::new());
    header_buffer
        .write_u32::<LittleEndian>(MAGIC)
        .map_err(DzipError::Io)?;
    header_buffer
        .write_u16::<LittleEndian>(config.files.len() as u16)
        .map_err(DzipError::Io)?;
    header_buffer
        .write_u16::<LittleEndian>(ctx.sorted_dirs.len() as u16)
        .map_err(DzipError::Io)?;
    header_buffer.write_u8(0).map_err(DzipError::Io)?;

    for f in &config.files {
        header_buffer
            .write_all(f.filename.as_bytes())
            .map_err(DzipError::Io)?;
        header_buffer.write_u8(0).map_err(DzipError::Io)?;
    }
    for d in ctx.sorted_dirs.iter().skip(1) {
        header_buffer
            .write_all(d.replace('/', "\\").as_bytes())
            .map_err(DzipError::Io)?;
        header_buffer.write_u8(0).map_err(DzipError::Io)?;
    }
    for f in &config.files {
        let raw_d = f.directory.replace('\\', "/");
        let d_key = if raw_d.is_empty() || raw_d == CURRENT_DIR_STR {
            CURRENT_DIR_STR
        } else {
            &raw_d
        };
        let d_id = *ctx.dir_map.get(d_key).unwrap_or(&0) as u16;
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
        .write_u16::<LittleEndian>((1 + config.archive_files.len()) as u16)
        .map_err(DzipError::Io)?;
    header_buffer
        .write_u16::<LittleEndian>(config.chunks.len() as u16)
        .map_err(DzipError::Io)?;

    let chunk_table_start = header_buffer.position();

    for _ in 0..config.chunks.len() {
        for _ in 0..16 {
            header_buffer.write_u8(0).map_err(DzipError::Io)?;
        }
    }

    if !config.archive_files.is_empty() {
        for fname in &config.archive_files {
            header_buffer
                .write_all(fname.as_bytes())
                .map_err(DzipError::Io)?;
            header_buffer.write_u8(0).map_err(DzipError::Io)?;
        }
    }

    if ctx.has_dz_chunk {
        if let Some(rs) = &config.range_settings {
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
    mut sorted_chunks_def: Vec<ChunkDef>,
    ctx: &PackContext,
    mut writer0: BufWriter<File>,
    mut split_writers: HashMap<u16, BufWriter<File>>,
    registry: &CodecRegistry,
    observer: &dyn DzipObserver,
) -> Result<(Vec<ChunkDef>, BufWriter<File>)> {
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
                ctx.chunk_source_map.get(&c_def.id).ok_or_else(|| {
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
    // We use a sync channel for backpressure
    let (tx, rx) = mpsc::sync_channel::<(usize, Result<Vec<u8>>)>(channel_bound);

    let mut current_offset_0 = ctx.current_offset_0;

    let mut split_offsets_owned: HashMap<u16, u32> =
        split_writers.keys().map(|k| (*k, 0)).collect();

    // IMPORTANT: Use std::thread::scope (Rust 1.63+) to borrow stack variables safely.
    std::thread::scope(|s| {
        // [Critical Fix]: Added `move` keyword.
        // `rx` is !Sync (Receiver cannot be shared by reference), so we MUST move it into the closure.
        let writer_handle = s.spawn(move || -> Result<(Vec<ChunkDef>, BufWriter<File>)> {
            let total_chunks = sorted_chunks_def.len();
            let mut buffer: HashMap<usize, Vec<u8>> = HashMap::new();
            let mut next_idx = 0;

            while next_idx < total_chunks {
                // Check if we already have the next needed chunk in buffer
                let data = if let Some(d) = buffer.remove(&next_idx) {
                    d
                } else {
                    // Receive from the channel
                    match rx.recv() {
                        Ok((idx, res)) => {
                            let chunk_data = res?;
                            if idx == next_idx {
                                chunk_data
                            } else {
                                // Received out of order, buffer it
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

                // Determine target writer (main or split)
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

                // Determine current position
                let current_pos = if c_def.archive_file_index == 0 {
                    current_offset_0
                } else {
                    *split_offsets_owned
                        .get(&c_def.archive_file_index)
                        .unwrap_or(&0)
                };

                // Write compressed data
                target_writer.write_all(&data).map_err(DzipError::Io)?;

                // Update chunk definition
                c_def.offset = current_pos;
                c_def.size_compressed = data.len() as u32;

                // Advance offset
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

                // Update progress bar via observer
                observer.progress_inc(1);
            }

            observer.progress_finish("Done");

            for w in split_writers.values_mut() {
                w.flush().map_err(DzipError::Io)?;
            }

            Ok((sorted_chunks_def, writer0))
        });

        // Producer (Parallel Compression)
        // Rayon handles thread management here
        jobs.par_iter().for_each_with(tx, |s, job| {
            let res = (|| -> Result<Vec<u8>> {
                let mut f_in = File::open(job.source_path.as_ref()).map_err(|e| {
                    DzipError::IoContext(
                        format!("Failed to read source chunk file {:?}", job.source_path),
                        e,
                    )
                })?;

                f_in.seek(SeekFrom::Start(job.offset))
                    .map_err(DzipError::Io)?;
                let mut chunk_reader =
                    BufReader::with_capacity(DEFAULT_BUFFER_SIZE, f_in).take(job.read_len as u64);
                let mut compressed_buffer = Vec::new();
                let flags_int = encode_flags(&job.flags);

                // registry.compress now returns DzipError, so we can propagate it directly with ?
                registry.compress(&mut chunk_reader, &mut compressed_buffer, flags_int)?;

                Ok(compressed_buffer)
            })();
            // Send result to writer thread
            let _ = s.send((job.chunk_idx, res));
        });

        // Wait for writer thread to finish
        writer_handle
            .join()
            .map_err(|e| DzipError::ThreadPanic(format!("{:?}", e)))?
    })
}

fn write_final_chunk_table(
    writer: &mut BufWriter<File>,
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
