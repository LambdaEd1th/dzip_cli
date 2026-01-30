use anyhow::{Context, Result};
use dzip_core::CompressionMethod;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DzipConfig {
    pub archives: Vec<String>,
    pub base_dir: PathBuf,
    pub files: Vec<FileEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<GlobalOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub archive_file_index: u16,
    pub compression: CompressionMethod,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub modifiers: String, // e.g., "to 25%"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalOptions {
    pub method: String,
    pub max_mem_usage: i32,
    pub use_combuf: bool,
    pub preprocess: bool,
    pub win_size: u8,
    pub offset_table_size: u8,
    pub offset_tables: u8,
    pub offset_contexts: u8,
    pub ref_length_table_size: u8,
    pub ref_length_tables: u8,
    pub ref_offset_table_size: u8,
    pub ref_offset_tables: u8,
    pub big_min_match: u8,
}

impl Default for GlobalOptions {
    fn default() -> Self {
        Self {
            method: "dz".to_string(),
            max_mem_usage: -1,
            use_combuf: false,
            preprocess: true,
            win_size: 16,
            offset_table_size: 8,
            offset_tables: 3,
            offset_contexts: 3,
            ref_length_table_size: 7,
            ref_length_tables: 1,
            ref_offset_table_size: 7,
            ref_offset_tables: 3,
            big_min_match: 15,
        }
    }
}

pub fn parse_config(path: &Path) -> Result<DzipConfig> {
    let content = std::fs::read_to_string(path)?;

    if path.extension().is_some_and(|ext| ext == "toml") {
        return Ok(toml::from_str(&content)?);
    }

    let mut config = DzipConfig {
        archives: Vec::new(),
        base_dir: PathBuf::from("."),
        files: Vec::new(),
        options: Some(GlobalOptions::default()),
    };

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "archive" => {
                if parts.len() > 1 {
                    config.archives.push(parts[1].to_string());
                }
            }
            "basedir" => {
                if parts.len() > 1 {
                    config.base_dir = PathBuf::from(parts[1]);
                }
            }
            "file" => {
                // file <path> <index> <algo> [modifiers...]
                if parts.len() >= 4 {
                    let path = dzip_core::path::resolve_relative_path(parts[1])
                        .context("Failed to resolve file path")?;
                    let idx = parts[2]
                        .parse::<u16>()
                        .context("Failed to parse archive index")?;
                    let algo = parts[3]
                        .parse::<CompressionMethod>()
                        .context("Failed to parse compression method")?;

                    let modifiers = if parts.len() > 4 {
                        parts[4..].join(" ")
                    } else {
                        String::new()
                    };

                    config.files.push(FileEntry {
                        path,
                        archive_file_index: idx,
                        compression: algo,
                        modifiers,
                    });
                }
            }
            "options" => {
                if parts.len() > 1 {
                    config.options.as_mut().unwrap().method = parts[1].to_string();
                }
            }
            "max_mem_usage" => {
                if parts.len() > 1 {
                    config.options.as_mut().unwrap().max_mem_usage = parts[1].parse().unwrap_or(-1);
                }
            }
            "use_combuf" => {
                if parts.len() > 1 {
                    config.options.as_mut().unwrap().use_combuf = parts[1] == "1";
                }
            }
            "preprocess" => {
                if parts.len() > 1 {
                    config.options.as_mut().unwrap().preprocess = parts[1] == "1";
                }
            }
            "winsize" => {
                if parts.len() > 1 {
                    config.options.as_mut().unwrap().win_size = parts[1].parse().unwrap_or(16);
                }
            }
            // Parse remaining specific options based on file
            key => {
                // Simple parser for other keys mapping directly to struct fields if names match loosely
                // For now, implementing manually for known fields to correspond to DerbhCLI.txt
                match key.to_lowercase().as_str() {
                    "offsettablesize" => {
                        config.options.as_mut().unwrap().offset_table_size =
                            parts[1].parse().unwrap_or(8)
                    }
                    "offsettables" => {
                        config.options.as_mut().unwrap().offset_tables =
                            parts[1].parse().unwrap_or(3)
                    }
                    "offsetcontexts" => {
                        config.options.as_mut().unwrap().offset_contexts =
                            parts[1].parse().unwrap_or(3)
                    }
                    "reflengthtablesize" => {
                        config.options.as_mut().unwrap().ref_length_table_size =
                            parts[1].parse().unwrap_or(7)
                    }
                    "reflengthtables" => {
                        config.options.as_mut().unwrap().ref_length_tables =
                            parts[1].parse().unwrap_or(1)
                    }
                    "refoffsettablesize" => {
                        config.options.as_mut().unwrap().ref_offset_table_size =
                            parts[1].parse().unwrap_or(7)
                    }
                    "refoffsettables" => {
                        config.options.as_mut().unwrap().ref_offset_tables =
                            parts[1].parse().unwrap_or(3)
                    }
                    "bigminmatch" => {
                        config.options.as_mut().unwrap().big_min_match =
                            parts[1].parse().unwrap_or(15)
                    }
                    _ => {
                        // Unknown option, for now just ignore or log
                        // eprintln!("Warning: Unknown config key '{}'", key);
                    }
                }
            }
        }
    }

    Ok(config)
}
