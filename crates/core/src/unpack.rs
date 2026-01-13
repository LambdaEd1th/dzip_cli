use byteorder::{LittleEndian, ReadBytesExt};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf, MAIN_SEPARATOR_STR};

use crate::compression::CodecRegistry;
use crate::constants::{
    ChunkFlags, CHUNK_LIST_TERMINATOR, CURRENT_DIR_STR, DEFAULT_BUFFER_SIZE, MAGIC,
};
use crate::error::DzipError;
use crate::types::{ArchiveMeta, ChunkDef, Config, FileEntry, RangeSettings};
use crate::utils::{decode_flags, read_null_term_string, sanitize_path};
use crate::{DzipObserver, Result};

// --- Internal Structures ---

struct ArchiveContext {
    version: u8,
    num_files: u16,
    num_dirs: u16,
    user_files: Vec<String>,
    directories: Vec<String>,
    map_entries: Vec<FileMapEntry>,
    chunks: Vec<RawChunk>,
    split_file_names: Vec<String>,
    range_settings: Option<RangeSettings>,
    main_file_len: u64,
}

struct FileMapEntry {
    id: usize,
    dir_idx: usize,
    chunk_ids: Vec<u16>,
}

#[derive(Clone)]
struct RawChunk {
    id: u16,
    offset: u32,
    _head_c_len: u32,
    d_len: u32,
    flags: u16,
    file_idx: u16,
    real_c_len: u32,
}

// --- Main Entry Point ---

pub fn do_unpack(
    input_path: &Path,
    out_opt: Option<PathBuf>,
    keep_raw: bool,
    registry: &CodecRegistry,
    observer: &dyn DzipObserver,
) -> Result<()> {
    // Phase 1: Read archive structure
    let mut ctx = read_archive_structure(input_path, observer)?;

    // Phase 2: Fix legacy chunk sizes
    correct_chunk_sizes(&mut ctx, input_path)?;

    // Determine output directory
    let base_name = input_path
        .file_stem()
        .ok_or_else(|| DzipError::Generic("Invalid input file path: no stem".to_string()))?
        .to_string_lossy();
    let root_out = out_opt.unwrap_or_else(|| PathBuf::from(base_name.to_string()));
    fs::create_dir_all(&root_out)
        .map_err(|e| DzipError::IoContext(format!("Creating out dir {:?}", root_out), e))?;

    // Phase 3: Extract files
    extract_files(&ctx, &root_out, input_path, keep_raw, registry, observer)?;

    // Phase 4: Generate config for repacking
    save_config(&ctx, &base_name, observer)?;

    Ok(())
}

// --- Sub-functions ---

fn read_archive_structure(
    input_path: &Path,
    observer: &dyn DzipObserver,
) -> Result<ArchiveContext> {
    let main_file_raw = File::open(input_path).map_err(|e| {
        DzipError::IoContext(format!("Failed to open main archive {:?}", input_path), e)
    })?;
    let main_file_len = main_file_raw.metadata().map_err(DzipError::Io)?.len();
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
        // Using map_err for basic IO errors during read
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

    Ok(ArchiveContext {
        version,
        num_files,
        num_dirs,
        user_files,
        directories,
        map_entries,
        chunks,
        split_file_names,
        range_settings,
        main_file_len,
    })
}

fn correct_chunk_sizes(ctx: &mut ArchiveContext, input_path: &Path) -> Result<()> {
    let base_dir = input_path.parent().unwrap_or(Path::new(CURRENT_DIR_STR));
    let mut file_chunks_map: HashMap<u16, Vec<usize>> = HashMap::new();

    for (idx, c) in ctx.chunks.iter().enumerate() {
        file_chunks_map.entry(c.file_idx).or_default().push(idx);
    }

    for (f_idx, c_indices) in file_chunks_map.iter() {
        let mut sorted_indices = c_indices.clone();
        sorted_indices.sort_by_key(|&i| ctx.chunks[i].offset);

        let current_file_size = if *f_idx == 0 {
            ctx.main_file_len
        } else {
            let idx = (*f_idx - 1) as usize;
            let split_name = ctx.split_file_names.get(idx).ok_or_else(|| {
                DzipError::Generic(format!("Invalid split file index {} in header", f_idx))
            })?;
            let split_path = base_dir.join(split_name);
            fs::metadata(&split_path)
                .map_err(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        DzipError::SplitFileMissing(split_path.clone())
                    } else {
                        DzipError::Io(e)
                    }
                })?
                .len()
        };

        for k in 0..sorted_indices.len() {
            let idx = sorted_indices[k];
            let current_offset = ctx.chunks[idx].offset;
            let next_offset = if k == sorted_indices.len() - 1 {
                current_file_size as u32
            } else {
                ctx.chunks[sorted_indices[k + 1]].offset
            };

            if next_offset < current_offset {
                ctx.chunks[idx].real_c_len = ctx.chunks[idx]._head_c_len;
            } else {
                ctx.chunks[idx].real_c_len = next_offset - current_offset;
            }
        }
    }
    Ok(())
}

fn extract_files(
    ctx: &ArchiveContext,
    root_out: &Path,
    input_path: &Path,
    keep_raw: bool,
    registry: &CodecRegistry,
    observer: &dyn DzipObserver,
) -> Result<()> {
    observer.info(&format!(
        "Extracting {} files to {:?}...",
        ctx.map_entries.len(),
        root_out
    ));

    let base_dir = input_path.parent().unwrap_or(Path::new(CURRENT_DIR_STR));

    let chunk_indices: HashMap<u16, usize> = ctx
        .chunks
        .iter()
        .enumerate()
        .map(|(i, c)| (c.id, i))
        .collect();

    observer.progress_start(ctx.map_entries.len() as u64);

    // Use Rayon for parallel extraction
    ctx.map_entries.par_iter().try_for_each_init(
        HashMap::new,
        |file_cache: &mut HashMap<u16, File>, entry| -> Result<()> {
            let fname = &ctx.user_files[entry.id];
            let raw_dir = if entry.dir_idx < ctx.directories.len() {
                &ctx.directories[entry.dir_idx]
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
                fs::create_dir_all(parent).map_err(DzipError::Io)?;
            }
            let out_file = File::create(&disk_path)
                .map_err(|e| DzipError::IoContext(format!("Creating file {:?}", disk_path), e))?;
            let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, out_file);

            for cid in &entry.chunk_ids {
                if let Some(&idx) = chunk_indices.get(cid) {
                    let chunk = &ctx.chunks[idx];

                    let source_file = match file_cache.entry(chunk.file_idx) {
                        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            let f = if chunk.file_idx == 0 {
                                File::open(input_path).map_err(|e| DzipError::IoContext(
                                    format!("Failed to open main archive {:?}", input_path), 
                                    e
                                ))?
                            } else {
                                let split_idx = (chunk.file_idx - 1) as usize;
                                let split_name =
                                    ctx.split_file_names.get(split_idx).ok_or_else(|| {
                                        DzipError::Generic(format!(
                                            "Invalid archive file index {} for chunk {}",
                                            chunk.file_idx, chunk.id
                                        ))
                                    })?;
                                let split_path = base_dir.join(split_name);
                                File::open(&split_path).map_err(|e| {
                                    if e.kind() == std::io::ErrorKind::NotFound {
                                        DzipError::SplitFileMissing(split_path.clone())
                                    } else {
                                        DzipError::IoContext(
                                            format!("Failed to open split file {:?}", split_path), 
                                            e
                                        )
                                    }
                                })?
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
                             return Err(e); // DzipError propagates directly
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

fn save_config(ctx: &ArchiveContext, base_name: &str, observer: &dyn DzipObserver) -> Result<()> {
    let mut toml_files = Vec::new();
    for entry in &ctx.map_entries {
        let fname = &ctx.user_files[entry.id];
        let raw_dir = if entry.dir_idx < ctx.directories.len() {
            &ctx.directories[entry.dir_idx]
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
    let mut sorted_chunks = ctx.chunks.clone();
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
            version: ctx.version,
            total_files: ctx.num_files,
            total_directories: ctx.num_dirs,
            total_chunks: ctx.chunks.len() as u16,
        },
        archive_files: ctx.split_file_names.clone(),
        range_settings: ctx.range_settings.clone(),
        files: toml_files,
        chunks: toml_chunks,
    };

    let config_path = format!("{}.toml", base_name);
    // Convert toml serialization errors
    let toml_str = toml::to_string_pretty(&config).map_err(DzipError::TomlSer)?;
    fs::write(&config_path, toml_str).map_err(DzipError::Io)?;

    observer.info(&format!("Unpack complete. Config saved to {}", config_path));
    Ok(())
}
