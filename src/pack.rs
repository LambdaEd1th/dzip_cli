use anyhow::{Result, anyhow};
use byteorder::{LittleEndian, WriteBytesExt};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{MAIN_SEPARATOR_STR, PathBuf};

use crate::compression::compress_data;
use crate::constants::{CHUNK_DZ, MAGIC};
use crate::types::{ChunkDef, Config};
use crate::utils::encode_flags;

pub fn do_pack(config_path: &PathBuf) -> Result<()> {
    let toml_content = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&toml_content)?;

    let base_dir = config_path
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let base_path = config_path.parent().unwrap().join(&base_dir);

    println!(
        "[Info] Packing from directory: {:?} using config...",
        base_path
    );

    let mut chunk_map: HashMap<u16, &ChunkDef> = HashMap::new();
    let mut has_dz_chunk = false;

    for c in &config.chunks {
        chunk_map.insert(c.id, c);
        let flags = encode_flags(&c.flags);
        if flags & CHUNK_DZ != 0 {
            has_dz_chunk = true;
        }
    }

    let mut encoded_chunks: HashMap<u16, Vec<u8>> = HashMap::new();

    // Process files
    for f_entry in &config.files {
        // FIX: Combine replace calls into one
        let rel_path = f_entry.path.replace(['\\', '/'], MAIN_SEPARATOR_STR);

        let prefix = format!(".{}", MAIN_SEPARATOR_STR);
        let clean_path = if rel_path.starts_with(&prefix) {
            &rel_path[2..]
        } else {
            &rel_path
        };

        let full_path = base_path.join(clean_path);
        if !full_path.exists() {
            return Err(anyhow!("Source file not found: {:?}", full_path));
        }

        let file_bytes = fs::read(&full_path)?;
        let mut offset = 0;

        for cid in &f_entry.chunks {
            let c_def = chunk_map
                .get(cid)
                .ok_or(anyhow!("Chunk definition missing"))?;
            let size = c_def.size_decompressed as usize;

            if offset + size > file_bytes.len() {
                return Err(anyhow!(
                    "File size mismatch for {:?}. Chunk {} needs {} bytes",
                    full_path,
                    cid,
                    size
                ));
            }

            let raw_slice = &file_bytes[offset..offset + size];
            offset += size;

            if !encoded_chunks.contains_key(cid) {
                let flags_int = encode_flags(&c_def.flags);
                let comp_data = compress_data(raw_slice, flags_int)?;
                encoded_chunks.insert(*cid, comp_data);
            }
        }
    }

    // 2. Build Directory List
    let mut unique_dirs = HashSet::new();
    for f in &config.files {
        let d = f.directory.trim();
        if d.is_empty() {
            unique_dirs.insert("".to_string());
        } else {
            unique_dirs.insert(d.replace('\\', "/"));
        }
    }
    if !unique_dirs.contains("") {
        unique_dirs.insert("".to_string());
    }

    let mut sorted_dirs: Vec<String> = unique_dirs.into_iter().filter(|d| !d.is_empty()).collect();
    sorted_dirs.sort();
    sorted_dirs.insert(0, "".to_string());

    let dir_map: HashMap<String, usize> = sorted_dirs
        .iter()
        .enumerate()
        .map(|(i, d)| (d.clone(), i))
        .collect();

    // 3. Begin Write Header to Memory (File 0)
    let out_filename_0 = format!("{}_packed.dz", base_dir);
    let mut header_writer = Cursor::new(Vec::new());

    // Header
    header_writer.write_u32::<LittleEndian>(MAGIC)?;
    header_writer.write_u16::<LittleEndian>(config.files.len() as u16)?;
    header_writer.write_u16::<LittleEndian>(sorted_dirs.len() as u16)?;
    header_writer.write_u8(0)?;

    // Strings: Files
    for f in &config.files {
        header_writer.write_all(f.filename.as_bytes())?;
        header_writer.write_u8(0)?;
    }

    // Strings: Dirs (Skip index 0 "")
    for d in sorted_dirs.iter().skip(1) {
        let win_path = d.replace('/', "\\");
        header_writer.write_all(win_path.as_bytes())?;
        header_writer.write_u8(0)?;
    }

    // Mapping Table
    for f in &config.files {
        let d_key = if f.directory.is_empty() {
            ""
        } else {
            &f.directory
        }
        .replace('\\', "/");

        let d_id = *dir_map.get(&d_key).unwrap_or(&0) as u16;
        header_writer.write_u16::<LittleEndian>(d_id)?;
        for cid in &f.chunks {
            header_writer.write_u16::<LittleEndian>(*cid)?;
        }
        writer_write_u16_end(&mut header_writer)?;
    }

    // Chunk Settings
    let num_arch_files = 1 + config.archive_files.len() as u16;
    header_writer.write_u16::<LittleEndian>(num_arch_files)?;
    header_writer.write_u16::<LittleEndian>(config.chunks.len() as u16)?;

    // --- Calculate Offsets ---
    let header_len = header_writer.position() as u32;
    let chunk_table_size = (config.chunks.len() * 16) as u32;

    // Calculate File List Size
    let mut file_list_size = 0;
    for fname in &config.archive_files {
        file_list_size += fname.len() as u32 + 1; // + null terminator
    }

    let range_settings_size = if has_dz_chunk { 10 } else { 0 };

    let mut data_offset_0 = header_len + chunk_table_size + file_list_size + range_settings_size;
    let mut data_offset_others: HashMap<u16, u32> = HashMap::new();
    for i in 1..num_arch_files {
        data_offset_others.insert(i, 0);
    }

    let mut sorted_chunks_def = config.chunks.clone();
    sorted_chunks_def.sort_by_key(|c| c.id);

    // Write Chunk Table
    for c in &sorted_chunks_def {
        let data = encoded_chunks.get(&c.id).unwrap();
        let comp_len = data.len() as u32;
        let file_idx = c.archive_file_index;

        let current_offset = if file_idx == 0 {
            data_offset_0
        } else {
            *data_offset_others.get(&file_idx).unwrap_or(&0)
        };

        header_writer.write_u32::<LittleEndian>(current_offset)?;
        header_writer.write_u32::<LittleEndian>(comp_len)?;
        header_writer.write_u32::<LittleEndian>(c.size_decompressed)?;
        header_writer.write_u16::<LittleEndian>(encode_flags(&c.flags))?;
        header_writer.write_u16::<LittleEndian>(file_idx)?;

        if file_idx == 0 {
            data_offset_0 += comp_len;
        } else {
            *data_offset_others.get_mut(&file_idx).unwrap() += comp_len;
        }
    }

    // Write File List
    if !config.archive_files.is_empty() {
        println!(
            "[Info] Writing File List ({} split files)...",
            config.archive_files.len()
        );
        for fname in &config.archive_files {
            header_writer.write_all(fname.as_bytes())?;
            header_writer.write_u8(0)?;
        }
    }

    // Write Range Settings
    if has_dz_chunk {
        if let Some(rs) = &config.range_settings {
            header_writer.write_u8(rs.win_size)?;
            header_writer.write_u8(rs.flags)?;
            header_writer.write_u8(rs.offset_table_size)?;
            header_writer.write_u8(rs.offset_tables)?;
            header_writer.write_u8(rs.offset_contexts)?;
            header_writer.write_u8(rs.ref_length_table_size)?;
            header_writer.write_u8(rs.ref_length_tables)?;
            header_writer.write_u8(rs.ref_offset_table_size)?;
            header_writer.write_u8(rs.ref_offset_tables)?;
            header_writer.write_u8(rs.big_min_match)?;
        } else {
            for _ in 0..10 {
                header_writer.write_u8(0)?;
            }
        }
    }

    // --- Flush Header to File 0 ---
    println!("[Info] Writing main archive: {}", out_filename_0);
    let mut file0 = File::create(&out_filename_0)?;
    file0.write_all(header_writer.get_ref())?;

    // --- Write Data Blobs ---
    let mut split_writers: HashMap<u16, File> = HashMap::new();

    for (i, fname) in config.archive_files.iter().enumerate() {
        let idx = (i + 1) as u16;
        let path = config_path.parent().unwrap().join(fname);
        println!("[Info] Creating split archive: {:?}", path);
        let f = File::create(&path)?;
        split_writers.insert(idx, f);
    }

    for c in &sorted_chunks_def {
        let data = encoded_chunks.get(&c.id).unwrap();
        let writer = if c.archive_file_index == 0 {
            &mut file0
        } else {
            split_writers
                .get_mut(&c.archive_file_index)
                .expect("Missing split file writer")
        };
        writer.write_all(data)?;
    }

    println!("[Success] All files packed.");
    Ok(())
}

fn writer_write_u16_end(w: &mut Cursor<Vec<u8>>) -> Result<()> {
    w.write_u16::<LittleEndian>(0xFFFF)?;
    Ok(())
}
