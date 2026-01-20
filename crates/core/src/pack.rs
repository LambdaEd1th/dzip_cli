use binrw::{BinWrite, NullString};
use log::info;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc;
use tempfile::NamedTempFile;

use crate::Result;
use crate::codec::compress;
use crate::error::DzipError;
use crate::format::{
    ArchiveHeader, CURRENT_DIR_STR, ChunkDiskEntry, ChunkFlags, ChunkTableHeader,
    DEFAULT_BUFFER_SIZE, FileMapDiskEntry, MAGIC, RangeSettingsDisk,
};
use crate::io::{PackSink, PackSource, WriteSeekSend};
use crate::model::{ChunkDef, Config};
use crate::utils::{encode_flags, to_archive_path};

type BoxedWriter = Box<dyn WriteSeekSend>;
type PipelineOutput = (Vec<ChunkDef>, BufWriter<BoxedWriter>);

pub struct PackPlan {
    pub config: Config,
    pub base_dir_name: String,
    pub chunk_source_map: HashMap<u16, (String, u64, usize)>,
    pub sorted_dirs: Vec<String>,
    pub dir_map: HashMap<String, usize>,
    pub has_dz_chunk: bool,
}

struct WriterContext {
    main: BufWriter<BoxedWriter>,
    split: HashMap<u16, BufWriter<BoxedWriter>>,
}

struct CompressionJob {
    chunk_idx: usize,
    source_path: String,
    offset: u64,
    read_len: usize,
    flag: String,
}

pub fn do_pack(
    config: Config,
    base_dir_name: String,
    sink: &mut dyn PackSink,
    source: &dyn PackSource,
    on_progress: impl Fn(crate::ProgressEvent) + Send + Sync,
) -> Result<()> {
    let plan = PackPlan::build(config, base_dir_name, source)?;
    plan.execute(sink, source, on_progress)?;
    Ok(())
}

impl PackPlan {
    pub fn build(config: Config, base_dir_name: String, source: &dyn PackSource) -> Result<Self> {
        info!("Indexing source files...");
        let mut chunk_map_def = HashMap::new();
        let mut has_dz_chunk = false;

        for c in &config.chunks {
            chunk_map_def.insert(c.id, c.clone());
            let flags =
                ChunkFlags::from_bits_truncate(encode_flags(&[std::borrow::Cow::Borrowed(
                    c.flag.as_str(),
                )]));
            if flags.contains(ChunkFlags::DZ_RANGE) {
                has_dz_chunk = true;
            }
        }

        let mut chunk_source_map = HashMap::new();
        for f_entry in &config.files {
            let os_path_str = &f_entry.path;
            if !source.exists(os_path_str) {
                return Err(DzipError::Config(format!(
                    "Source file not found (path: {})",
                    os_path_str
                )));
            }

            // Assume single chunk per file, so offset starts at 0
            let current_offset: u64 = 0;
            let cid = f_entry.chunk;

            let c_def = chunk_map_def
                .get(&cid)
                .ok_or(DzipError::ChunkDefinitionMissing(cid))?;

            let flags =
                ChunkFlags::from_bits_truncate(encode_flags(&[std::borrow::Cow::Borrowed(
                    c_def.flag.as_str(),
                )]));

            let read_len = if flags.contains(ChunkFlags::DZ_RANGE) {
                c_def.size_compressed
            } else {
                c_def.size_decompressed
            } as usize;

            if read_len == 0 {
                log::warn!(
                    "Chunk {} has size 0. This may result in empty output. \
                     Consider setting size_decompressed in the config.",
                    cid
                );
            }

            chunk_source_map.insert(cid, (os_path_str.clone(), current_offset, read_len));
        }

        let mut unique_dirs = HashSet::new();
        for f in &config.files {
            let d = f.directory.trim();
            if d.is_empty() || d == CURRENT_DIR_STR {
                unique_dirs.insert(CURRENT_DIR_STR.to_string());
            } else {
                unique_dirs.insert(to_archive_path(Path::new(d)));
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
            base_dir_name,
            chunk_source_map,
            sorted_dirs,
            dir_map,
            has_dz_chunk,
        })
    }

    pub fn execute(
        &self,
        sink: &mut dyn PackSink,
        source: &dyn PackSource,
        on_progress: impl Fn(crate::ProgressEvent) + Send + Sync,
    ) -> Result<()> {
        info!("Packing logical archive: {:?}", self.base_dir_name);
        let mut writers = self.prepare_writers(sink)?;

        let chunk_table_start = self.build_and_write_header(&mut writers.main)?;

        let current_offset_0 = writers.main.stream_position().map_err(DzipError::Io)? as u32;

        let final_chunks = self.run_compression_pipeline(
            current_offset_0,
            writers.main,
            writers.split,
            source,
            on_progress,
        )?;

        let (updated_chunks, mut main_writer_final) = final_chunks;

        Self::write_final_chunk_table(&mut main_writer_final, chunk_table_start, &updated_chunks)?;

        info!("All files packed successfully.");
        Ok(())
    }

    fn prepare_writers(&self, sink: &mut dyn PackSink) -> Result<WriterContext> {
        let f0 = sink.create_main()?;
        let writer0 = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f0);
        let mut split_writers = HashMap::new();
        for (i, _) in self.config.archive_files.iter().enumerate() {
            let idx = (i + 1) as u16;
            let f = sink.create_split(idx)?;
            split_writers.insert(idx, BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f));
        }
        Ok(WriterContext {
            main: writer0,
            split: split_writers,
        })
    }

    fn build_and_write_header(&self, writer: &mut BufWriter<BoxedWriter>) -> Result<u64> {
        let mut header_buffer = Cursor::new(Vec::new());

        // 1. Write Header
        let header = ArchiveHeader {
            magic: MAGIC,
            num_files: self.config.files.len() as u16,
            num_dirs: self.sorted_dirs.len() as u16,
            version: 0,
        };
        header
            .write(&mut header_buffer)
            .map_err(|e| DzipError::Generic(format!("Header write failed: {}", e)))?;

        // 2. Write File Names
        for f in &self.config.files {
            NullString::from(f.filename.clone())
                .write(&mut header_buffer)
                .map_err(|e| DzipError::Generic(format!("Filename write failed: {}", e)))?;
        }

        // 3. Write Directory Names (skip root ".")
        for d in self.sorted_dirs.iter().skip(1) {
            NullString::from(d.clone())
                .write(&mut header_buffer)
                .map_err(|e| DzipError::Generic(format!("Directory name write failed: {}", e)))?;
        }

        // 4. Write File Map Entries
        for f in &self.config.files {
            let raw_d = to_archive_path(Path::new(&f.directory));
            let d_key = if raw_d.is_empty() || raw_d == CURRENT_DIR_STR {
                CURRENT_DIR_STR
            } else {
                &raw_d
            };
            let d_id = *self.dir_map.get(d_key).unwrap_or(&0) as u16;

            let entry = FileMapDiskEntry {
                dir_idx: d_id,
                chunk_ids: vec![f.chunk],
            };
            entry
                .write(&mut header_buffer)
                .map_err(|e| DzipError::Generic(format!("File map write failed: {}", e)))?;
        }

        // 5. Write Chunk Table Header
        let chunk_header = ChunkTableHeader {
            num_arch_files: (1 + self.config.archive_files.len()) as u16,
            num_chunks: self.config.chunks.len() as u16,
        };
        chunk_header
            .write(&mut header_buffer)
            .map_err(|e| DzipError::Generic(format!("Chunk header write failed: {}", e)))?;

        let chunk_table_start = header_buffer.position();

        // 6. Write Placeholder Chunk Table
        let dummy_chunk = ChunkDiskEntry {
            offset: 0,
            c_len: 0,
            d_len: 0,
            flags: 0,
            file_idx: 0,
        };
        for _ in 0..self.config.chunks.len() {
            dummy_chunk.write(&mut header_buffer).map_err(|e| {
                DzipError::Generic(format!("Chunk placeholder write failed: {}", e))
            })?;
        }

        // 7. Write Split Filenames
        for fname in &self.config.archive_files {
            NullString::from(fname.clone())
                .write(&mut header_buffer)
                .map_err(|e| DzipError::Generic(format!("Split filename write failed: {}", e)))?;
        }

        // 8. Write Range Settings
        if self.has_dz_chunk {
            if let Some(rs) = &self.config.range_settings {
                let rs_disk = RangeSettingsDisk {
                    win_size: rs.win_size,
                    flags: rs.flags,
                    offset_table_size: rs.offset_table_size,
                    offset_tables: rs.offset_tables,
                    offset_contexts: rs.offset_contexts,
                    ref_length_table_size: rs.ref_length_table_size,
                    ref_length_tables: rs.ref_length_tables,
                    ref_offset_table_size: rs.ref_offset_table_size,
                    ref_offset_tables: rs.ref_offset_tables,
                    big_min_match: rs.big_min_match,
                };
                rs_disk.write(&mut header_buffer).map_err(|e| {
                    DzipError::Generic(format!("Range settings write failed: {}", e))
                })?;
            } else {
                // Manual zero padding if missing.
                for _ in 0..10 {
                    0u8.write(&mut header_buffer)
                        .map_err(|e| DzipError::Generic(format!("Padding write failed: {}", e)))?;
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
        mut writer0: BufWriter<BoxedWriter>,
        mut split_writers: HashMap<u16, BufWriter<BoxedWriter>>,
        source: &dyn PackSource,
        on_progress: impl Fn(crate::ProgressEvent) + Send + Sync,
    ) -> Result<PipelineOutput> {
        let mut sorted_chunks_def = self.config.chunks.clone();
        sorted_chunks_def.sort_by_key(|c| c.id);
        info!("Compressing {} chunks ...", sorted_chunks_def.len());
        on_progress(crate::ProgressEvent::Start(sorted_chunks_def.len()));

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
                    flag: c_def.flag.clone(),
                })
            })
            .collect();
        let jobs = jobs?;

        let channel_bound = rayon::current_num_threads() * 4;
        let (tx, rx) = mpsc::sync_channel::<(usize, Result<(NamedTempFile, u64)>)>(channel_bound);
        let mut current_offset_0 = start_offset_0;
        let mut split_offsets_owned: HashMap<u16, u32> =
            split_writers.keys().map(|k| (*k, 0)).collect();

        std::thread::scope(|s| {
            let writer_handle = s.spawn(move || -> Result<PipelineOutput> {
                let total_chunks = sorted_chunks_def.len();
                let mut buffer: HashMap<usize, (NamedTempFile, u64)> = HashMap::new();
                let mut next_idx = 0;
                while next_idx < total_chunks {
                    let (mut temp_file, actual_d_len) = if let Some(d) = buffer.remove(&next_idx) {
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
                                    "Compression threads disconnected".into(),
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

                    let compressed_size =
                        std::io::copy(&mut temp_file, target_writer).map_err(DzipError::Io)?;

                    c_def.offset = current_pos;
                    c_def.size_compressed = compressed_size as u32;
                    c_def.size_decompressed = actual_d_len as u32;

                    if c_def.archive_file_index == 0 {
                        current_offset_0 += c_def.size_compressed;
                    } else {
                        let offset = split_offsets_owned
                            .get_mut(&c_def.archive_file_index)
                            .ok_or_else(|| {
                                DzipError::InternalLogic(format!(
                                    "Split offset missing for index {}",
                                    c_def.archive_file_index
                                ))
                            })?;
                        *offset += c_def.size_compressed;
                    }
                    next_idx += 1;
                    on_progress(crate::ProgressEvent::Inc(1));
                }

                for w in split_writers.values_mut() {
                    w.flush().map_err(DzipError::Io)?;
                }
                on_progress(crate::ProgressEvent::Finish);
                Ok((sorted_chunks_def, writer0))
            });

            jobs.par_iter().for_each_with(tx, |s, job| {
                let res = (|| -> Result<(NamedTempFile, u64)> {
                    let mut f_in = source.open_file(&job.source_path)?;
                    f_in.seek(SeekFrom::Start(job.offset))
                        .map_err(DzipError::Io)?;
                    let chunk_reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, f_in)
                        .take(job.read_len as u64);
                    let mut counting_reader = CountingReader {
                        inner: chunk_reader,
                        count: 0,
                    };
                    let mut temp_file = NamedTempFile::new().map_err(DzipError::Io)?;

                    let flags_int = encode_flags(&[std::borrow::Cow::Borrowed(job.flag.as_str())]);

                    compress(&mut counting_reader, &mut temp_file, flags_int)?;
                    temp_file.seek(SeekFrom::Start(0)).map_err(DzipError::Io)?;
                    Ok((temp_file, counting_reader.count))
                })();
                let _ = s.send((job.chunk_idx, res));
            });
            writer_handle
                .join()
                .map_err(|e| DzipError::ThreadPanic(format!("{:?}", e)))?
        })
    }

    fn write_final_chunk_table(
        writer: &mut BufWriter<BoxedWriter>,
        table_start_pos: u64,
        chunks: &[ChunkDef],
    ) -> Result<()> {
        let mut table_buffer = Cursor::new(Vec::new());
        for c in chunks {
            let flags_int = encode_flags(&[std::borrow::Cow::Borrowed(c.flag.as_str())]);

            let disk_entry = ChunkDiskEntry {
                offset: c.offset,
                c_len: c.size_compressed,
                d_len: c.size_decompressed,
                flags: flags_int,
                file_idx: c.archive_file_index,
            };

            disk_entry.write(&mut table_buffer).map_err(|e| {
                DzipError::Generic(format!("Final chunk table write failed: {}", e))
            })?;
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

struct CountingReader<R> {
    inner: R,
    count: u64,
}
impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count += n as u64;
        Ok(n)
    }
}
