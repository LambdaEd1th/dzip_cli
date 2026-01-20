use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub archive: ArchiveMeta,
    pub archive_files: Vec<String>,
    pub range_settings: Option<RangeSettings>,
    pub files: Vec<FileEntry>,
    pub chunks: Vec<ChunkDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveMeta {
    pub version: u8,
    pub total_files: u16,
    pub total_directories: u16,
    pub total_chunks: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path relative to the extraction root (OS-specific separators)
    pub path: String,
    /// Directory path (OS-specific separators)
    pub directory: String,
    /// Filename without path
    pub filename: String,
    /// The single chunk ID associated with this file
    pub chunk: u16,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkDef {
    pub id: u16,
    pub offset: u32,
    pub size_compressed: u32,
    pub size_decompressed: u32,
    pub flag: String,
    pub archive_file_index: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeSettings {
    pub win_size: u8,
    pub flags: u8,
    pub offset_table_size: u8,
    pub offset_tables: u8,
    pub offset_contexts: u8,
    pub ref_length_table_size: u8,
    pub ref_length_tables: u8,
    pub ref_offset_table_size: u8,
    pub ref_offset_tables: u8,
    pub big_min_match: u8,
}
