use byteorder::{LittleEndian, WriteBytesExt};
use log::info;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc;
use tempfile::NamedTempFile;

use crate::Result;
use crate::codecs::compress;
use crate::error::DzipError;
use crate::format::{
    CHUNK_LIST_TERMINATOR, CURRENT_DIR_STR, ChunkFlags, DEFAULT_BUFFER_SIZE, MAGIC,
};
use crate::io::{PackSink, PackSource, WriteSeekSend};
use crate::model::{ChunkDef, Config};
use crate::utils::{encode_flags, to_archive_path}; // Changed import: use archive path

// (Keep Types, PackPlan struct, WriterContext, CompressionJob, wrapper function unchanged)
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
    flags: Vec<std::borrow::Cow<'static, str>>,
}

pub fn do_pack(
    config: Config,
    base_dir_name: String,
    sink: &mut dyn PackSink,
    source: &dyn PackSource,
) -> Result<()> {
    let plan = PackPlan::build(config, base_dir_name, source)?;
    plan.execute(sink, source)?;
    Ok(())
}

impl PackPlan {
    pub fn build(config: Config, base_dir_name: String, source: &dyn PackSource) -> Result<Self> {
        info!("Indexing source files...");
        // (Chunk definition loading map code unchanged)
        let mut chunk_map_def = HashMap::new();
        let mut has_dz_chunk = false;
        for c in &config.chunks {
            chunk_map_def.insert(c.id, c.clone());
            let flags_cow: Vec<std::borrow::Cow<'_, str>> = c
                .flags
                .iter()
                .map(|s| std::borrow::Cow::Borrowed(s.as_str()))
                .collect();
            let flags = ChunkFlags::from_bits_truncate(encode_flags(&flags_cow));
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
            let mut current_offset: u64 = 0;
            for cid in &f_entry.chunks {
                let c_def = chunk_map_def
                    .get(cid)
                    .ok_or(DzipError::ChunkDefinitionMissing(*cid))?;
                let flags_cow: Vec<std::borrow::Cow<'_, str>> = c_def
                    .flags
                    .iter()
                    .map(|s| std::borrow::Cow::Borrowed(s.as_str()))
                    .collect();
                let flags = ChunkFlags::from_bits_truncate(encode_flags(&flags_cow));
                let read_len = if flags.contains(ChunkFlags::DZ_RANGE) {
                    c_def.size_compressed
                } else {
                    c_def.size_decompressed
                } as usize;
                chunk_source_map.insert(*cid, (os_path_str.clone(), current_offset, read_len));
                current_offset += read_len as u64;
            }
        }

        let mut unique_dirs = HashSet::new();
        for f in &config.files {
            let d = f.directory.trim();
            if d.is_empty() || d == CURRENT_DIR_STR {
                unique_dirs.insert(CURRENT_DIR_STR.to_string());
            } else {
                // [Changed] Use to_archive_path!
                // Even if the input 'd' comes from a Windows TOML (with backslashes),
                // to_archive_path will convert it to "a/b/c" so the header is correct.
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

    pub fn execute(&self, sink: &mut dyn PackSink, source: &dyn PackSource) -> Result<()> {
        info!("Packing logical archive: {:?}", self.base_dir_name);
        let mut writers = self.prepare_writers(sink)?;
        let chunk_table_start = self.build_and_write_header(&mut writers.main)?;
        let current_offset_0 = writers.main.stream_position().map_err(DzipError::Io)? as u32;
        let final_chunks =
            self.run_compression_pipeline(current_offset_0, writers.main, writers.split, source)?;
        let (updated_chunks, mut main_writer_final) = final_chunks;
        Self::write_final_chunk_table(&mut main_writer_final, chunk_table_start, &updated_chunks)?;
        info!("All files packed successfully.");
        Ok(())
    }

    // (prepare_writers unchanged)
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
        header_buffer
            .write_u32::<LittleEndian>(MAGIC)
            .map_err(DzipError::Io)?;
        header_buffer
            .write_u16::<LittleEndian>(self.config.files.len() as u16)
            .map_err(DzipError::Io)?;
        header_buffer
            .write_u16::<LittleEndian>(self.sorted_dirs.len() as u16)
            .map_err(DzipError::Io)?;
        header_buffer.write_u8(0).map_err(DzipError::Io)?;
        for f in &self.config.files {
            header_buffer
                .write_all(f.filename.as_bytes())
                .map_err(DzipError::Io)?;
            header_buffer.write_u8(0).map_err(DzipError::Io)?;
        }
        for d in self.sorted_dirs.iter().skip(1) {
            // These are already normalized to archive format by build()
            header_buffer
                .write_all(d.as_bytes())
                .map_err(DzipError::Io)?;
            header_buffer.write_u8(0).map_err(DzipError::Io)?;
        }
        for f in &self.config.files {
            // [Changed] Convert file directory to archive format to look up the ID
            let raw_d = to_archive_path(Path::new(&f.directory));
            let d_key = if raw_d.is_empty() || raw_d == CURRENT_DIR_STR {
                CURRENT_DIR_STR
            } else {
                &raw_d
            };
            let d_id = *self.dir_map.get(d_key).unwrap_or(&0) as u16;

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
        // (Rest of header writing unchanged)
        header_buffer
            .write_u16::<LittleEndian>((1 + self.config.archive_files.len()) as u16)
            .map_err(DzipError::Io)?;
        header_buffer
            .write_u16::<LittleEndian>(self.config.chunks.len() as u16)
            .map_err(DzipError::Io)?;
        let chunk_table_start = header_buffer.position();
        for _ in 0..self.config.chunks.len() {
            for _ in 0..16 {
                header_buffer.write_u8(0).map_err(DzipError::Io)?;
            }
        }
        if !self.config.archive_files.is_empty() {
            for fname in &self.config.archive_files {
                header_buffer
                    .write_all(fname.as_bytes())
                    .map_err(DzipError::Io)?;
                header_buffer.write_u8(0).map_err(DzipError::Io)?;
            }
        }
        if self.has_dz_chunk {
            if let Some(rs) = &self.config.range_settings {
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

    // (run_compression_pipeline and write_final_chunk_table unchanged, they just copy data)
    fn run_compression_pipeline(
        &self,
        start_offset_0: u32,
        mut writer0: BufWriter<BoxedWriter>,
        mut split_writers: HashMap<u16, BufWriter<BoxedWriter>>,
        source: &dyn PackSource,
    ) -> Result<PipelineOutput> {
        // (Keep existing implementation...)
        // Note: Since run_compression_pipeline is long and logic unchanged, I will omit full repetition here for brevity unless requested.
        // Just ensure it uses the struct fields which are already processed correctly.
        // ... (Logic same as previous version)
        // OK to copy previous implementation completely.

        // Shortened for context display:
        let mut sorted_chunks_def = self.config.chunks.clone();
        sorted_chunks_def.sort_by_key(|c| c.id);
        info!("Compressing {} chunks ...", sorted_chunks_def.len());
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
                    flags: c_def
                        .flags
                        .iter()
                        .map(|s| std::borrow::Cow::Owned(s.clone()))
                        .collect(),
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
                }
                for w in split_writers.values_mut() {
                    w.flush().map_err(DzipError::Io)?;
                }
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
                    let flags_cow: Vec<std::borrow::Cow<'_, str>> = job
                        .flags
                        .iter()
                        .map(|c| std::borrow::Cow::Borrowed(c.as_ref()))
                        .collect();
                    let flags_int = encode_flags(&flags_cow);
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
        // (Keep existing implementation...)
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
            let flags_cow: Vec<std::borrow::Cow<'_, str>> = c
                .flags
                .iter()
                .map(|s| std::borrow::Cow::Borrowed(s.as_str()))
                .collect();
            table_buffer
                .write_u16::<LittleEndian>(encode_flags(&flags_cow))
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
}

// (Helper Structs CountingReader unchanged)
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
