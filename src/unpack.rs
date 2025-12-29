use byteorder::{LittleEndian, ReadBytesExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{MAIN_SEPARATOR_STR, Path, PathBuf};

use crate::Result;
use crate::compression::CodecRegistry;
use crate::constants::{CHUNK_LIST_TERMINATOR, ChunkFlags, DEFAULT_BUFFER_SIZE, MAGIC};
use crate::error::DzipError;
use crate::types::{ArchiveMeta, ChunkDef, Config, FileEntry, RangeSettings};
use crate::utils::{decode_flags, read_null_term_string, sanitize_path};

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
) -> Result<()> {
    let mut ctx = read_archive_structure(input_path)?;
    correct_chunk_sizes(&mut ctx, input_path)?;

    let base_name = input_path
        .file_stem()
        .ok_or_else(|| DzipError::Generic("Invalid input file path: no stem".to_string()))?
        .to_string_lossy();
    let root_out = out_opt.unwrap_or_else(|| PathBuf::from(base_name.to_string()));
    fs::create_dir_all(&root_out)
        .map_err(|e| DzipError::IoContext(format!("Creating out dir {:?}", root_out), e))?;

    extract_files(&ctx, &root_out, input_path, keep_raw, registry)?;
    save_config(&ctx, &base_name, &root_out)?;

    Ok(())
}

// --- Sub-functions ---

fn read_archive_structure(input_path: &Path) -> Result<ArchiveContext> {
    let main_file_raw = File::open(input_path).map_err(|e| {
        DzipError::IoContext(format!("Failed to open main archive {:?}", input_path), e)
    })?;
    let main_file_len = main_file_raw.metadata()?.len();
    let mut reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, main_file_raw);

    let magic = reader.read_u32::<LittleEndian>()?;
    if magic != MAGIC {
        return Err(DzipError::InvalidMagic(magic));
    }
    let num_files = reader.read_u16::<LittleEndian>()?;
    let num_dirs = reader.read_u16::<LittleEndian>()?;
    let version = reader.read_u8()?;

    info!(
        "Header: Ver {}, Files {}, Dirs {}",
        version, num_files, num_dirs
    );

    let mut user_files = Vec::with_capacity(num_files as usize);
    for _ in 0..num_files {
        // read_null_term_string now returns io::Result, so mapping to DzipError::Io is correct
        user_files.push(read_null_term_string(&mut reader).map_err(DzipError::Io)?);
    }

    let mut directories = Vec::with_capacity(num_dirs as usize);
    directories.push(".".to_string());
    for _ in 0..(num_dirs - 1) {
        directories.push(read_null_term_string(&mut reader).map_err(DzipError::Io)?);
    }

    let mut map_entries = Vec::with_capacity(num_files as usize);
    for i in 0..num_files {
        let dir_id = reader.read_u16::<LittleEndian>()? as usize;
        let mut chunk_ids = Vec::new();
        loop {
            let cid = reader.read_u16::<LittleEndian>()?;
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

    let num_arch_files = reader.read_u16::<LittleEndian>()?;
    let num_chunks = reader.read_u16::<LittleEndian>()?;
    info!(
        "Chunk Settings: {} chunks in {} archive files",
        num_chunks, num_arch_files
    );

    let mut chunks = Vec::with_capacity(num_chunks as usize);
    let mut has_dz_chunk = false;

    for i in 0..num_chunks {
        let offset = reader.read_u32::<LittleEndian>()?;
        let c_len = reader.read_u32::<LittleEndian>()?;
        let d_len = reader.read_u32::<LittleEndian>()?;
        let flags_raw = reader.read_u16::<LittleEndian>()?;
        let file_idx = reader.read_u16::<LittleEndian>()?;

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
        info!("Reading {} split archive filenames...", num_arch_files - 1);
        for _ in 0..(num_arch_files - 1) {
            split_file_names.push(read_null_term_string(&mut reader).map_err(DzipError::Io)?);
        }
    }

    let mut range_settings = None;
    if has_dz_chunk {
        info!("Detected CHUNK_DZ, reading RangeSettings...");
        range_settings = Some(RangeSettings {
            win_size: reader.read_u8()?,
            flags: reader.read_u8()?,
            offset_table_size: reader.read_u8()?,
            offset_tables: reader.read_u8()?,
            offset_contexts: reader.read_u8()?,
            ref_length_table_size: reader.read_u8()?,
            ref_length_tables: reader.read_u8()?,
            ref_offset_table_size: reader.read_u8()?,
            ref_offset_tables: reader.read_u8()?,
            big_min_match: reader.read_u8()?,
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
    let base_dir = input_path.parent().unwrap_or(Path::new("."));
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
    _keep_raw: bool,
    registry: &CodecRegistry,
) -> Result<()> {
    info!(
        "Extracting {} files to {:?}...",
        ctx.map_entries.len(),
        root_out
    );

    let base_dir = input_path.parent().unwrap_or(Path::new("."));

    let chunk_indices: HashMap<u16, usize> = ctx
        .chunks
        .iter()
        .enumerate()
        .map(|(i, c)| (c.id, i))
        .collect();

    let pb = ProgressBar::new(ctx.map_entries.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    ctx.map_entries.par_iter().try_for_each_init(
        HashMap::new,
        |file_cache: &mut HashMap<u16, File>, entry| -> Result<()> {
            let fname = &ctx.user_files[entry.id];
            let raw_dir = if entry.dir_idx < ctx.directories.len() {
                &ctx.directories[entry.dir_idx]
            } else {
                "."
            };
            let full_raw_path = if raw_dir == "." || raw_dir.is_empty() {
                fname.clone()
            } else {
                format!("{}/{}", raw_dir, fname)
            };

            // [Fixed]: sanitize_path now returns crate::Result, so just ? works
            let disk_path = sanitize_path(root_out, &full_raw_path)?;

            if let Some(parent) = disk_path.parent() {
                fs::create_dir_all(parent)?;
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
                                File::open(input_path)?
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
                                        DzipError::Io(e)
                                    }
                                })?
                            };
                            e.insert(f)
                        }
                    };

                    source_file.seek(SeekFrom::Start(chunk.offset as u64))?;

                    let mut source_reader =
                        BufReader::with_capacity(DEFAULT_BUFFER_SIZE, source_file)
                            .take(chunk.real_c_len as u64);

                    if let Err(e) = registry.decompress(
                        &mut source_reader,
                        &mut writer,
                        chunk.flags,
                        chunk.d_len,
                    ) {
                        let err_msg = e.to_string();

                        let mut raw_buf_reader = source_reader.into_inner();
                        raw_buf_reader.seek(SeekFrom::Start(chunk.offset as u64))?;
                        let mut raw_take = raw_buf_reader.take(chunk.real_c_len as u64);

                        warn!(
                            "Failed to decompress chunk {}: {}. Writing raw data.",
                            chunk.id, err_msg
                        );
                        std::io::copy(&mut raw_take, &mut writer)?;
                    }
                }
            }
            writer.flush()?;
            pb.inc(1);
            Ok(())
        },
    )?;

    pb.finish_with_message("Done");
    Ok(())
}

fn save_config(ctx: &ArchiveContext, base_name: &str, _root_out: &Path) -> Result<()> {
    let mut toml_files = Vec::new();
    for entry in &ctx.map_entries {
        let fname = &ctx.user_files[entry.id];
        let raw_dir = if entry.dir_idx < ctx.directories.len() {
            &ctx.directories[entry.dir_idx]
        } else {
            "."
        };
        let full_raw_path = if raw_dir == "." || raw_dir.is_empty() {
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
    fs::write(&config_path, toml::to_string_pretty(&config)?)?;
    info!("Unpack complete. Config saved to {}", config_path);
    Ok(())
}
