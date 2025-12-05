use anyhow::{Result, anyhow};
use byteorder::{LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{MAIN_SEPARATOR, PathBuf};

use crate::compression::decompress_chunk;
use crate::constants::{CHUNK_DZ, MAGIC};
use crate::types::{ArchiveMeta, ChunkDef, Config, FileEntry, RangeSettings};
use crate::utils::{decode_flags, read_null_term_string};

pub fn do_unpack(input_path: &PathBuf, out_opt: Option<PathBuf>) -> Result<()> {
    let mut main_file = File::open(input_path)?;
    let main_file_len = main_file.metadata()?.len();

    // 1. Read Header
    let magic = main_file.read_u32::<LittleEndian>()?;
    if magic != MAGIC {
        return Err(anyhow!("Invalid Magic: expected DTRZ"));
    }
    let num_files = main_file.read_u16::<LittleEndian>()?;
    let num_dirs = main_file.read_u16::<LittleEndian>()?;
    let version = main_file.read_u8()?;

    println!(
        "[Info] Header: Ver {}, Files {}, Dirs {}",
        version, num_files, num_dirs
    );

    // 2. Read Strings
    let mut user_files = Vec::new();
    for _ in 0..num_files {
        user_files.push(read_null_term_string(&mut main_file)?);
    }

    let mut directories = Vec::new();
    // Rule: First directory is implicit "."
    directories.push(".".to_string());
    for _ in 0..(num_dirs - 1) {
        directories.push(read_null_term_string(&mut main_file)?);
    }

    // 3. Mapping Table
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
            if cid == 0xFFFF {
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

    // 4. Chunk Settings
    let num_arch_files = main_file.read_u16::<LittleEndian>()?;
    let num_chunks = main_file.read_u16::<LittleEndian>()?;
    println!(
        "[Info] Chunk Settings: {} chunks in {} archive files",
        num_chunks, num_arch_files
    );

    // 5. Chunk List
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
    let mut chunks = Vec::new();
    let mut has_dz_chunk = false;

    for i in 0..num_chunks {
        let offset = main_file.read_u32::<LittleEndian>()?;
        let c_len = main_file.read_u32::<LittleEndian>()?;
        let d_len = main_file.read_u32::<LittleEndian>()?;
        let flags = main_file.read_u16::<LittleEndian>()?;
        let file_idx = main_file.read_u16::<LittleEndian>()?;

        if flags & CHUNK_DZ != 0 {
            has_dz_chunk = true;
        }

        chunks.push(RawChunk {
            id: i,
            offset,
            _head_c_len: c_len,
            d_len,
            flags,
            file_idx,
            real_c_len: 0,
        });
    }

    // 6. File List (Multiple Archive Support)
    let mut split_file_names = Vec::new();
    if num_arch_files > 1 {
        println!(
            "[Info] Reading {} split archive filenames...",
            num_arch_files - 1
        );
        for _ in 0..(num_arch_files - 1) {
            split_file_names.push(read_null_term_string(&mut main_file)?);
        }
    }

    // 7. Decoder Settings
    let mut range_settings_opt = None;
    if has_dz_chunk {
        println!("[Info] Detected CHUNK_DZ, reading RangeSettings...");
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

    // --- Prepare ---
    let base_dir = input_path.parent().unwrap_or(std::path::Path::new("."));

    // Calculate sizes per file
    let mut file_chunks_map: HashMap<u16, Vec<usize>> = HashMap::new();
    for (idx, c) in chunks.iter().enumerate() {
        file_chunks_map.entry(c.file_idx).or_default().push(idx);
    }

    for (f_idx, c_indices) in file_chunks_map.iter() {
        let mut sorted_indices = c_indices.clone();
        sorted_indices.sort_by_key(|&i| chunks[i].offset);

        let current_file_size = if *f_idx == 0 {
            main_file_len
        } else {
            let split_name = &split_file_names[(*f_idx - 1) as usize];
            let split_path = base_dir.join(split_name);
            fs::metadata(&split_path)?.len()
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
                chunks[idx].real_c_len = chunks[idx]._head_c_len; // Fallback
            } else {
                chunks[idx].real_c_len = next_offset - current_offset;
            }
        }
    }

    let mut chunk_map = HashMap::new();
    for c in &chunks {
        chunk_map.insert(c.id, c.clone());
    }

    // Output setup
    let base_name = input_path.file_stem().unwrap().to_string_lossy();
    let root_out = out_opt.unwrap_or_else(|| PathBuf::from(&base_name.to_string()));
    fs::create_dir_all(&root_out)?;

    // 7. Extraction
    println!("[Info] Extracting {} files...", map_entries.len());
    let mut toml_files = Vec::new();

    let mut split_handles: HashMap<u16, File> = HashMap::new();

    for entry in &map_entries {
        let fname = &user_files[entry.id];
        let raw_dir = if entry.dir_idx < directories.len() {
            &directories[entry.dir_idx]
        } else {
            "."
        };

        let system_dir = raw_dir
            .replace('\\', &MAIN_SEPARATOR.to_string())
            .replace('/', &MAIN_SEPARATOR.to_string());
        let final_dir = if system_dir == "." {
            ".".to_string()
        } else {
            system_dir
        };

        let rel_path = if final_dir == "." {
            fname.clone()
        } else {
            format!(
                "{}{}{}",
                final_dir.trim_end_matches(MAIN_SEPARATOR),
                MAIN_SEPARATOR,
                fname
            )
        };

        let disk_path = root_out.join(&rel_path);
        if let Some(parent) = disk_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file_buf = Vec::new();
        for cid in &entry.chunk_ids {
            if let Some(chunk) = chunk_map.get(cid) {
                // Reader selection
                let reader: &mut File = if chunk.file_idx == 0 {
                    &mut main_file
                } else {
                    if !split_handles.contains_key(&chunk.file_idx) {
                        let f_name = &split_file_names[(chunk.file_idx - 1) as usize];
                        let f_path = base_dir.join(f_name);
                        let f = File::open(&f_path)
                            .map_err(|e| anyhow!("Missing split file {:?}: {}", f_path, e))?;
                        split_handles.insert(chunk.file_idx, f);
                    }
                    split_handles.get_mut(&chunk.file_idx).unwrap()
                };

                reader.seek(SeekFrom::Start(chunk.offset as u64))?;
                let mut raw_data = vec![0u8; chunk.real_c_len as usize];
                reader.read_exact(&mut raw_data)?;

                match decompress_chunk(&raw_data, chunk.flags, chunk.d_len) {
                    Ok(decompressed) => file_buf.extend_from_slice(&decompressed),
                    Err(e) => {
                        eprintln!("[Warn] Failed to decompress file {}: {}", rel_path, e);
                        file_buf.extend_from_slice(&raw_data);
                    }
                }
            }
        }
        fs::write(&disk_path, &file_buf)?;

        toml_files.push(FileEntry {
            path: rel_path,
            directory: final_dir,
            filename: fname.clone(),
            chunks: entry.chunk_ids.clone(),
        });
    }

    // 8. Write Config
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

    let toml_str = toml::to_string_pretty(&config)?;
    let config_path = format!("{}.toml", base_name);
    fs::write(&config_path, toml_str)?;

    println!(
        "[Success] Extracted to {:?} and saved config to {}",
        root_out, config_path
    );

    Ok(())
}
