use crate::error::{DzipError, Result};
use crate::format::*;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{BufRead, BufReader, Read, Seek};

pub struct DzipReader<R: Read + Seek> {
    reader: BufReader<R>,
}

impl<R: Read + Seek> DzipReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    pub fn read_archive_settings(&mut self) -> Result<ArchiveSettings> {
        log::debug!(
            "Reading archive settings at offset {}",
            self.reader.stream_position().unwrap_or(0)
        );
        let header = self.reader.read_u32::<LittleEndian>()?;
        if header != 0x5A525444 {
            // 'DTRZ' in little endian (ZRTD)
            return Err(DzipError::InvalidHeader);
        }

        let num_user_files = self.reader.read_u16::<LittleEndian>()?;
        let num_directories = self.reader.read_u16::<LittleEndian>()?;
        let version = self.reader.read_u8()?;

        Ok(ArchiveSettings {
            header,
            num_user_files,
            num_directories,
            version,
        })
    }

    pub fn read_strings(&mut self, count: usize) -> Result<Vec<String>> {
        log::debug!(
            "Reading {} strings from offset {}",
            count,
            self.reader.stream_position().unwrap_or(0)
        );
        let mut strings = Vec::with_capacity(count);
        for _ in 0..count {
            let s = self.read_null_terminated_string()?;
            // log::debug!("String: '{}'", s);
            strings.push(s);
        }
        Ok(strings)
    }

    fn read_null_terminated_string(&mut self) -> Result<String> {
        let mut bytes = Vec::new();
        let _ = self.reader.read_until(0, &mut bytes)?;
        if bytes.last() == Some(&0) {
            bytes.pop();
        }
        Ok(String::from_utf8(bytes)?)
    }

    /// Reads the User-File to Chunk-And-Directory list.
    /// Returns a vector of tuples: (Directory ID, List of Chunk IDs).
    pub fn read_file_chunk_map(&mut self, num_files: usize) -> Result<Vec<(u16, Vec<u16>)>> {
        log::debug!("Reading file chunk map for {} files", num_files);
        let mut map = Vec::with_capacity(num_files);
        for _ in 0..num_files {
            let dir_id = self.reader.read_u16::<LittleEndian>()?;
            let mut chunks = Vec::new();
            loop {
                let chunk_id = self.reader.read_u16::<LittleEndian>()?;
                if chunk_id == 0xFFFF {
                    break;
                }
                chunks.push(chunk_id);
            }
            map.push((dir_id, chunks));
        }
        Ok(map)
    }

    pub fn read_chunk_settings(&mut self) -> Result<ChunkSettings> {
        let num_archive_files = self.reader.read_u16::<LittleEndian>()?;
        let num_chunks = self.reader.read_u16::<LittleEndian>()?;
        Ok(ChunkSettings {
            num_archive_files,
            num_chunks,
        })
    }

    pub fn read_chunks(&mut self, count: usize) -> Result<Vec<Chunk>> {
        log::debug!(
            "Reading {} chunks from offset {}",
            count,
            self.reader.stream_position().unwrap_or(0)
        );
        let mut chunks = Vec::with_capacity(count);
        for _ in 0..count {
            let offset = self.reader.read_u32::<LittleEndian>()?;
            let compressed_length = self.reader.read_u32::<LittleEndian>()?;
            let decompressed_length = self.reader.read_u32::<LittleEndian>()?;
            let flags = self.reader.read_u16::<LittleEndian>()?;
            let file = self.reader.read_u16::<LittleEndian>()?;
            chunks.push(Chunk {
                offset,
                compressed_length,
                decompressed_length,
                flags,
                file,
            });
        }
        Ok(chunks)
    }

    pub fn read_global_settings(&mut self) -> Result<RangeSettings> {
        let win_size = self.reader.read_u8()?;
        let flags = self.reader.read_u8()?;
        let offset_table_size = self.reader.read_u8()?;
        let offset_tables = self.reader.read_u8()?;
        let offset_contexts = self.reader.read_u8()?;
        let ref_length_table_size = self.reader.read_u8()?;
        let ref_length_tables = self.reader.read_u8()?;
        let ref_offset_table_size = self.reader.read_u8()?;
        let ref_offset_tables = self.reader.read_u8()?;
        let big_min_match = self.reader.read_u8()?;

        Ok(RangeSettings {
            win_size,
            flags,
            offset_table_size,
            offset_tables,
            offset_contexts,
            ref_length_table_size,
            ref_length_tables,
            ref_offset_table_size,
            ref_offset_tables,
            big_min_match,
        })
    }

    pub fn read_file_list(&mut self, num_archive_files: usize) -> Result<Vec<String>> {
        let mut files = Vec::with_capacity(num_archive_files);
        for _ in 0..num_archive_files {
            files.push(self.read_null_terminated_string()?);
        }
        Ok(files)
    }

    pub fn position(&mut self) -> std::io::Result<u64> {
        self.reader.stream_position()
    }

    pub fn read_chunk_data(&mut self, chunk: &Chunk) -> Result<Vec<u8>> {
        Self::decompress_chunk_data(&mut self.reader, chunk)
    }

    pub fn read_chunk_data_with_volumes(
        &mut self,
        chunk: &Chunk,
        volume_source: &mut dyn VolumeSource,
    ) -> Result<Vec<u8>> {
        if chunk.file == 0 {
            Self::decompress_chunk_data(&mut self.reader, chunk)
        } else {
            let reader = volume_source.open_volume(chunk.file)?;
            Self::decompress_chunk_data(reader, chunk)
        }
    }

    fn decompress_chunk_data(reader: &mut dyn ReadSeek, chunk: &Chunk) -> Result<Vec<u8>> {
        log::trace!(
            "Decompressing Chunk: offset={}, comp={}, decomp={}, flags={:x}",
            chunk.offset,
            chunk.compressed_length,
            chunk.decompressed_length,
            chunk.flags
        );
        // Handle Zero chunk (optimization for empty/zeroed regions)
        // Must be handled before seeking, as offset might be invalid/virtual for zero chunks.
        if (chunk.flags & CHUNK_ZERO) != 0 {
            return Ok(vec![0u8; chunk.decompressed_length as usize]);
        }

        reader.seek(std::io::SeekFrom::Start(chunk.offset as u64))?;

        // Read compressed data
        let mut buffer = vec![0u8; chunk.compressed_length as usize];
        reader.read_exact(&mut buffer)?;

        // If explicitly flagged as copy encoded, or no compression flags set?
        // Actually, let's just check flags.
        // User confirmed: CHUNK_MP3 and CHUNK_JPEG are equivalent to CHUNK_COPYCOMP
        if (chunk.flags & (CHUNK_COPYCOMP | CHUNK_MP3 | CHUNK_JPEG)) != 0 {
            return Ok(buffer);
        }

        // Handle RandomAccess chunks (usually stored uncompressed if no other compression flag is set)
        if (chunk.flags & CHUNK_RANDOMACCESS) != 0 {
            // Check if any actual compression flag is ALSO set.
            // If LZMA/ZLIB/BZIP/DZ are NOT set, then it's just raw data with a type hint.
            if (chunk.flags & (CHUNK_LZMA | CHUNK_ZLIB | CHUNK_BZIP | CHUNK_DZ)) == 0 {
                return Ok(buffer);
            }
        }

        if (chunk.flags & CHUNK_ZLIB) != 0 {
            // Heuristic for "Equal Lengths" Quirk:
            if chunk.compressed_length == chunk.decompressed_length {
                // Typical Zlib header starts with 0x78 (Deflate, 32k win).
                // If it doesn't look like Zlib, assume raw.
                if buffer.is_empty() || (buffer[0] & 0x0F) != 0x08 {
                    // Low nibble 8 = Deflate.
                    // 0x78 is extremely common (CINFO=7 => 32k window).
                    // If not deflate, likely raw.
                    return Ok(buffer);
                }
            }

            // Check for GZIP header (0x1f 0x8b)
            if buffer.len() >= 2 && buffer[0] == 0x1f && buffer[1] == 0x8b {
                let mut decoder = flate2::read::GzDecoder::new(&buffer[..]);
                let mut decompressed = Vec::with_capacity(chunk.decompressed_length as usize);
                match std::io::Read::read_to_end(&mut decoder, &mut decompressed) {
                    Ok(_) => return Ok(decompressed),
                    Err(e) => {
                        // If we extracted the full expected length, ignore the error (likely missing footer)
                        if decompressed.len() == chunk.decompressed_length as usize {
                            return Ok(decompressed);
                        }
                        return Err(DzipError::Io(e));
                    }
                }
            }

            let mut decoder = flate2::read::ZlibDecoder::new(&buffer[..]);
            let mut decompressed = Vec::with_capacity(chunk.decompressed_length as usize);
            match std::io::Read::read_to_end(&mut decoder, &mut decompressed) {
                Ok(_) => return Ok(decompressed),
                Err(_) if chunk.compressed_length == chunk.decompressed_length => {
                    return Ok(buffer);
                }
                Err(e) => return Err(DzipError::Io(e)),
            }
        }

        if (chunk.flags & CHUNK_BZIP) != 0 {
            // Heuristic for "Equal Lengths" Quirk:
            if chunk.compressed_length == chunk.decompressed_length {
                // Bzip2 header must start with "BZh".
                if buffer.len() < 3 || &buffer[0..3] != b"BZh" {
                    return Ok(buffer);
                }
            }

            let mut decoder = bzip2::read::BzDecoder::new(&buffer[..]);
            let mut decompressed = Vec::with_capacity(chunk.decompressed_length as usize);
            match std::io::Read::read_to_end(&mut decoder, &mut decompressed) {
                Ok(_) => return Ok(decompressed),
                Err(_) if chunk.compressed_length == chunk.decompressed_length => {
                    return Ok(buffer);
                }
                Err(e) => return Err(DzipError::Io(e)),
            }
        }

        if (chunk.flags & CHUNK_LZMA) != 0 {
            // Heuristic for "Equal Lengths" Quirk ambiguity:
            if chunk.compressed_length == chunk.decompressed_length
                && (buffer.is_empty() || buffer[0] != 0x5d)
            {
                // Does not start with typical LZMA property byte. Likely Raw.
                return Ok(buffer);
            }

            let mut decompressed = Vec::with_capacity(chunk.decompressed_length as usize);
            let mut reader = std::io::Cursor::new(&buffer[..]);
            // lzma-rs usually handles LZMA headers automatically.
            match lzma_rs::lzma_decompress(&mut reader, &mut decompressed) {
                Ok(_) => return Ok(decompressed),
                Err(e) => {
                    let threshold = (chunk.compressed_length as f32 * 0.8) as usize;
                    if !decompressed.is_empty() && decompressed.len() > threshold {
                        eprintln!(
                            "WARN: LZMA decompression finished with error '{}' but produced {} bytes (> 80%). Returning partial data.",
                            e,
                            decompressed.len()
                        );
                        return Ok(decompressed);
                    }
                    if chunk.compressed_length == chunk.decompressed_length {
                        eprintln!(
                            "debug: LZMA failed with error '{}' but lengths match (fallback to raw).",
                            e
                        );
                        return Ok(buffer);
                    }
                    return Err(DzipError::Io(std::io::Error::other(e)));
                }
            }
        }

        // TODO: Implement other decompression methods (e.g. CHUNK_DZ)
        Err(DzipError::UnsupportedCompression(chunk.flags))
    }
}

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

pub trait VolumeSource {
    /// Open the volume with the given index (1-based, corresponding to the file list)
    fn open_volume(&mut self, id: u16) -> Result<&mut dyn ReadSeek>;
}

/// Corrects chunk sizes based on actual file boundaries.
///
/// Some archives (like testnew.dz) have incorrect compressed_length headers (e.g., listing uncompressed size).
/// This function clamps compressed lengths to the available space between chunks or EOF.
///
/// # Arguments
/// * `chunks` - The list of chunks to correct.
/// * `file_sizes` - specific file sizes mapped by file ID (0 for main, 1+ for volumes).
pub fn correct_chunk_sizes(
    chunks: &mut [crate::format::Chunk],
    file_sizes: &std::collections::HashMap<u16, u64>,
) {
    use crate::format::*;
    let mut chunks_by_file: std::collections::HashMap<u16, Vec<usize>> =
        std::collections::HashMap::new();
    for (i, chunk) in chunks.iter().enumerate() {
        chunks_by_file.entry(chunk.file).or_default().push(i);
    }

    for (file_id, mut indices) in chunks_by_file {
        indices.sort_by_key(|&i| chunks[i].offset);

        let file_size = *file_sizes.get(&file_id).unwrap_or(&0);

        for i in 0..indices.len() {
            let idx = indices[i];
            let chunk_offset = chunks[idx].offset as u64;

            // Determine the limit (end of region)
            let limit = if i + 1 < indices.len() {
                chunks[indices[i + 1]].offset as u64
            } else {
                file_size
            };

            let available = limit.saturating_sub(chunk_offset);

            // If header claims more than available, clamp it.
            // BMS Logic: If SIZE == ZSIZE (equal lengths) for compressed chunks, it means
            // the size is unknown/placeholder, so we SHOULD use the available size (next offset - current).
            let is_compressed =
                (chunks[idx].flags & (CHUNK_LZMA | CHUNK_ZLIB | CHUNK_BZIP | CHUNK_DZ)) != 0;
            let equal_sizes = chunks[idx].compressed_length == chunks[idx].decompressed_length;

            if is_compressed && equal_sizes {
                // Always update to available size (whether larger or smaller)
                if chunks[idx].compressed_length != available as u32 {
                    log::debug!(
                        "Correcting Equal-Size Chunk {} from {} to {} (File {}, Offset {})",
                        idx,
                        chunks[idx].compressed_length,
                        available,
                        chunks[idx].file,
                        chunk_offset
                    );
                    chunks[idx].compressed_length = available as u32;
                }
            } else if (chunks[idx].compressed_length as u64) > available {
                log::debug!(
                    "Correcting Chunk {} size from {} to {} (File {}, Offset {})",
                    idx,
                    chunks[idx].compressed_length,
                    available,
                    chunks[idx].file,
                    chunk_offset
                );
                chunks[idx].compressed_length = available as u32;
            }
        }
    }
}
