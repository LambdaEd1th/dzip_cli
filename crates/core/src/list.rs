use std::collections::HashMap;
use std::path::{MAIN_SEPARATOR_STR, Path};

use crate::constants::CURRENT_DIR_STR;
use crate::io::DzipFileSystem;
use crate::unpack::ArchiveMetadata;
use crate::{DzipObserver, Result};

/// Struct representing file information for listing.
/// Contains basic metadata useful for display.
pub struct ListEntry {
    pub path: String,
    pub original_size: u64,
    pub chunk_count: usize,
}

/// Lists the contents of a .dz archive without extracting or correcting chunk sizes.
/// This leverages ArchiveMetadata::load for fast, read-only access.
pub fn do_list(
    input_path: &Path,
    observer: &dyn DzipObserver,
    fs_impl: &dyn DzipFileSystem,
) -> Result<Vec<ListEntry>> {
    observer.info("Reading archive header...");

    // Phase 1: Load Metadata only (Fast, Read-Only)
    // We strictly use ArchiveMetadata here. No UnpackPlan is built,
    // so no split-file checks or chunk size corrections are performed.
    let meta = ArchiveMetadata::load(input_path, observer, fs_impl)?;

    observer.info(&format!(
        "Archive loaded. Version: {}, Files: {}, Dirs: {}",
        meta.version,
        meta.map_entries.len(),
        meta.directories.len()
    ));

    // Create a lookup for raw chunks to calculate file sizes.
    // We rely on 'd_len' (decompressed length) from the raw headers.
    let chunk_lookup: HashMap<u16, &crate::unpack::RawChunk> =
        meta.raw_chunks.iter().map(|c| (c.id, c)).collect();

    let mut entries = Vec::with_capacity(meta.map_entries.len());

    for entry in &meta.map_entries {
        // 1. Resolve full path
        let fname = &meta.user_files[entry.id];
        let raw_dir = if entry.dir_idx < meta.directories.len() {
            &meta.directories[entry.dir_idx]
        } else {
            CURRENT_DIR_STR
        };

        let full_path = if raw_dir == CURRENT_DIR_STR || raw_dir.is_empty() {
            fname.clone()
        } else {
            // Use standard separator for consistent display
            format!("{}{}{}", raw_dir, MAIN_SEPARATOR_STR, fname)
        };

        // 2. Calculate original (decompressed) size
        let mut total_size: u64 = 0;
        for cid in &entry.chunk_ids {
            if let Some(chunk) = chunk_lookup.get(cid) {
                total_size += chunk.d_len as u64;
            }
        }

        entries.push(ListEntry {
            path: full_path,
            original_size: total_size,
            chunk_count: entry.chunk_ids.len(),
        });
    }

    Ok(entries)
}
