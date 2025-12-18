use anyhow::{Context, Result, anyhow};
use byteorder::{LittleEndian, ReadBytesExt};
use log::{info, warn};
use std::collections::{HashMap, hash_map::Entry};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
// [Fix] Import MAIN_SEPARATOR_STR to adapt path separators for the current OS ( \ for Windows, / for Unix)
use std::path::{MAIN_SEPARATOR_STR, PathBuf};

use crate::compression::CodecRegistry;
use crate::constants::{CHUNK_LIST_TERMINATOR, ChunkFlags, MAGIC};
use crate::error::DzipError;
use crate::types::{ArchiveMeta, ChunkDef, Config, FileEntry, RangeSettings};
use crate::utils::{decode_flags, read_null_term_string, sanitize_path};

pub fn do_unpack(
    input_path: &PathBuf,
    out_opt: Option<PathBuf>,
    keep_raw: bool,
    registry: &CodecRegistry,
) -> Result<()> {
    // Open the main archive file
    let main_file_raw = File::open(input_path)
        .map_err(DzipError::Io)
        .context(format!("Failed to open main archive: {:?}", input_path))?;

    let main_file_len = main_file_raw.metadata()?.len();
    let mut main_file = BufReader::new(main_file_raw);

    // 1. Read Header
    let magic = main_file.read_u32::<LittleEndian>()?;
    if magic != MAGIC {
        return Err(DzipError::InvalidMagic(magic).into());
    }
    let num_files = main_file.read_u16::<LittleEndian>()?;
    let num_dirs = main_file.read_u16::<LittleEndian>()?;
    let version = main_file.read_u8()?;

    info!(
        "Header: Ver {}, Files {}, Dirs {}",
        version, num_files, num_dirs
    );

    // 2. Read String Table (Filenames)
    let mut user_files = Vec::new();
    for _ in 0..num_files {
        user_files.push(read_null_term_string(&mut main_file)?);
    }

    // Read String Table (Directories)
    let mut directories = Vec::new();
    directories.push(".".to_string());
    for _ in 0..(num_dirs - 1) {
        directories.push(read_null_term_string(&mut main_file)?);
    }

    // 3. Read Mapping Table
    struct FileMapEntry {
        id: usize,
        dir_idx: usize,
        chunk_ids: Vec<u16>,
    }
    let mut map_entries = Vec::new();
    for i in 0..num_files {
        let dir_id = main_file.read_u16::<LittleEndian>()? as usize;
        let mut chunks = Vec::new();
        loop {
            let cid = main_file.read_u16::<LittleEndian>()?;
            if cid == CHUNK_LIST_TERMINATOR {
                break;
            }
            chunks.push(cid);
        }
        map_entries.push(FileMapEntry {
            id: i as usize,
            dir_idx: dir_id,
            chunk_ids: chunks,
        });
    }

    // 4. Read Chunk Settings
    let num_arch_files = main_file.read_u16::<LittleEndian>()?;
    let num_chunks = main_file.read_u16::<LittleEndian>()?;
    info!(
        "Chunk Settings: {} chunks in {} archive files",
        num_chunks, num_arch_files
    );

    // 5. Read Chunk List
    #[derive(Clone)]
    struct RawChunk {
        id: u16,
        offset: u32,
        _head_c_len: u32, // Head compressed length (might be inaccurate in old archives)
        d_len: u32,
        flags: u16,
        file_idx: u16,
        real_c_len: u32, // Calculated real compressed length
    }
    let mut chunks = Vec::new();
    let mut has_dz_chunk = false;

    for i in 0..num_chunks {
        let offset = main_file.read_u32::<LittleEndian>()?;
        let c_len = main_file.read_u32::<LittleEndian>()?;
        let d_len = main_file.read_u32::<LittleEndian>()?;
        let flags_raw = main_file.read_u16::<LittleEndian>()?;
        let file_idx = main_file.read_u16::<LittleEndian>()?;

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

    // 6. Read Split Filenames
    let mut split_file_names = Vec::new();
    if num_arch_files > 1 {
        info!("Reading {} split archive filenames...", num_arch_files - 1);
        for _ in 0..(num_arch_files - 1) {
            split_file_names.push(read_null_term_string(&mut main_file)?);
        }
    }

    // 7. Read RangeSettings (if DZ chunk exists)
    let mut range_settings_opt = None;
    if has_dz_chunk {
        info!("Detected CHUNK_DZ, reading RangeSettings...");
        range_settings_opt = Some(RangeSettings {
            win_size: main_file.read_u8()?,
            flags: main_file.read_u8()?,
            offset_table_size: main_file.read_u8()?,
            offset_tables: main_file.read_u8()?,
            offset_contexts: main_file.read_u8()?,
            ref_length_table_size: main_file.read_u8()?,
            ref_length_tables: main_file.read_u8()?,
            ref_offset_table_size: main_file.read_u8()?,
            ref_offset_tables: main_file.read_u8()?,
            big_min_match: main_file.read_u8()?,
        });
    }

    // --- ZSIZE Correction (Calculate real compressed size) ---
    let base_dir = input_path.parent().unwrap_or(std::path::Path::new("."));
    let mut file_chunks_map: HashMap<u16, Vec<usize>> = HashMap::new();
    for (idx, c) in chunks.iter().enumerate() {
        file_chunks_map.entry(c.file_idx).or_default().push(idx);
    }

    for (f_idx, c_indices) in file_chunks_map.iter() {
        let mut sorted_indices = c_indices.clone();
        sorted_indices.sort_by_key(|&i| chunks[i].offset);

        // Get the size of the current archive part (main or split)
        let current_file_size = if *f_idx == 0 {
            main_file_len
        } else {
            let split_name = &split_file_names[(*f_idx - 1) as usize];
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

        // Calculate chunk size by subtracting offsets
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

    // [Optimization] Build Index Map (Chunk ID -> Vector Index) to avoid cloning
    let chunk_indices: HashMap<u16, usize> =
        chunks.iter().enumerate().map(|(i, c)| (c.id, i)).collect();

    let base_name = input_path
        .file_stem()
        .ok_or_else(|| anyhow!("Invalid input file path"))?
        .to_string_lossy();
    let root_out = out_opt.unwrap_or_else(|| PathBuf::from(&base_name.to_string()));
    fs::create_dir_all(&root_out)?;

    // 8. Start Extraction
    info!(
        "Extracting {} files to {:?}...",
        map_entries.len(),
        root_out
    );
    let mut toml_files = Vec::new();
    let mut split_handles: HashMap<u16, File> = HashMap::new();

    for entry in &map_entries {
        let fname = &user_files[entry.id];
        let raw_dir = if entry.dir_idx < directories.len() {
            &directories[entry.dir_idx]
        } else {
            "."
        };
        let full_raw_path = if raw_dir == "." || raw_dir.is_empty() {
            fname.clone()
        } else {
            format!("{}/{}", raw_dir, fname)
        };

        // 1. Physical extraction path: sanitize_path handles security and basic normalization
        let disk_path = sanitize_path(&root_out, &full_raw_path)?;

        // 2. TOML display path: Replace separators with the OS-specific one.
        // On Windows, this becomes "path\to\file". On Unix, "path/to/file".
        let rel_path_display = full_raw_path.replace(['/', '\\'], MAIN_SEPARATOR_STR);
        let dir_display = raw_dir.replace(['/', '\\'], MAIN_SEPARATOR_STR);

        if let Some(parent) = disk_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let out_file = File::create(&disk_path)?;
        let mut writer = BufWriter::new(out_file);

        for cid in &entry.chunk_ids {
            if let Some(&idx) = chunk_indices.get(cid) {
                let chunk = &chunks[idx];

                // Prepare source reader (Main file or Split file)
                let mut source_reader: Box<dyn Read> = if chunk.file_idx == 0 {
                    main_file.seek(SeekFrom::Start(chunk.offset as u64))?;
                    Box::new(main_file.by_ref().take(chunk.real_c_len as u64))
                } else {
                    match split_handles.entry(chunk.file_idx) {
                        Entry::Occupied(e) => {
                            let f = e.get();
                            let mut f_clone = f.try_clone()?;
                            f_clone.seek(SeekFrom::Start(chunk.offset as u64))?;
                            Box::new(f_clone.take(chunk.real_c_len as u64))
                        }
                        Entry::Vacant(e) => {
                            let f_name = &split_file_names[(chunk.file_idx - 1) as usize];
                            let f_path = base_dir.join(f_name);
                            let f = File::open(&f_path).map_err(|e| {
                                if e.kind() == std::io::ErrorKind::NotFound {
                                    DzipError::SplitFileMissing(f_path.clone())
                                } else {
                                    DzipError::Io(e)
                                }
                            })?;
                            let mut f_clone = f.try_clone()?;
                            f_clone.seek(SeekFrom::Start(chunk.offset as u64))?;
                            e.insert(f);
                            Box::new(f_clone.take(chunk.real_c_len as u64))
                        }
                    }
                };

                // Decompress
                if let Err(e) =
                    registry.decompress(&mut source_reader, &mut writer, chunk.flags, chunk.d_len)
                {
                    drop(source_reader);
                    let c_flags = ChunkFlags::from_bits_truncate(chunk.flags);

                    if c_flags.contains(ChunkFlags::DZ_RANGE) && keep_raw {
                        info!(
                            "Keeping raw data for chunk {} (DZ_RANGE) in {}",
                            chunk.id, rel_path_display
                        );
                        // Fallback: Copy raw data
                        let mut raw_reader: Box<dyn Read> = if chunk.file_idx == 0 {
                            main_file.seek(SeekFrom::Start(chunk.offset as u64))?;
                            Box::new(main_file.by_ref().take(chunk.real_c_len as u64))
                        } else {
                            let f = split_handles.get(&chunk.file_idx).unwrap();
                            let mut f_clone = f.try_clone()?;
                            f_clone.seek(SeekFrom::Start(chunk.offset as u64))?;
                            Box::new(f_clone.take(chunk.real_c_len as u64))
                        };
                        std::io::copy(&mut raw_reader, &mut writer)?;
                    } else if c_flags.contains(ChunkFlags::DZ_RANGE) {
                        return Err(DzipError::Unsupported(format!(
                            "Chunk format DZ_RANGE in {}. Use --keep-raw.",
                            rel_path_display
                        ))
                        .into());
                    } else {
                        warn!(
                            "Failed to decompress {}: {}. Writing raw data.",
                            rel_path_display, e
                        );
                        // Fallback: Copy raw data on failure
                        let mut raw_reader: Box<dyn Read> = if chunk.file_idx == 0 {
                            main_file.seek(SeekFrom::Start(chunk.offset as u64))?;
                            Box::new(main_file.by_ref().take(chunk.real_c_len as u64))
                        } else {
                            let f = split_handles.get(&chunk.file_idx).unwrap();
                            let mut f_clone = f.try_clone()?;
                            f_clone.seek(SeekFrom::Start(chunk.offset as u64))?;
                            Box::new(f_clone.take(chunk.real_c_len as u64))
                        };
                        std::io::copy(&mut raw_reader, &mut writer)?;
                    }
                }
            }
        }
        writer.flush()?;

        // 9. Save file info for TOML generation
        toml_files.push(FileEntry {
            path: rel_path_display, // Use OS-adaptive separator
            directory: dir_display, // Use OS-adaptive separator
            filename: fname.clone(),
            chunks: entry.chunk_ids.clone(),
        });
    }

    // 10. Generate TOML Config
    let mut toml_chunks = Vec::new();
    let mut sorted_chunks_for_toml = chunks;
    sorted_chunks_for_toml.sort_by_key(|c| c.id);

    for c in sorted_chunks_for_toml {
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
            version,
            total_files: num_files,
            total_directories: num_dirs,
            total_chunks: num_chunks,
        },
        archive_files: split_file_names,
        range_settings: range_settings_opt,
        files: toml_files,
        chunks: toml_chunks,
    };

    let config_path = format!("{}.toml", base_name);
    fs::write(&config_path, toml::to_string_pretty(&config)?)?;
    info!("Unpack complete. Config saved to {}", config_path);
    Ok(())
}
